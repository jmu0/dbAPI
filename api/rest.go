package api

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"

	"github.com/jmu0/dbAPI/db"
)

type requestData struct {
	SchemaName       string
	TableName        string
	PrimaryKeyValues []string
	Query            map[string]string
	FormData         map[string]string
}

func (rd *requestData) setPrimaryKeyValues(cols []db.Column) error {
	var keyCounter = 0
	for i, col := range cols {
		if col.PrimaryKey == true {
			if keyCounter > len(rd.PrimaryKeyValues)-1 {
				return errors.New("More primary key columns than values")
			}
			cols[i].Value = rd.PrimaryKeyValues[keyCounter]
			keyCounter++
		}
	}
	if keyCounter < len(rd.PrimaryKeyValues)-1 {
		return errors.New("More values than primary key columns")
	}
	return nil
}
func (rd *requestData) setColValues(cols []db.Column) error {
	if rd.FormData == nil {
		return errors.New("No form data")
	}
	for i, col := range cols {
		for k, v := range rd.FormData {
			if strings.ToLower(k) == strings.ToLower(col.Name) {
				cols[i].Value = v
			}
		}
	}
	return nil
}

//RestHandler returns a http HandleFunc
func RestHandler(pathPrefix string, c db.Conn) func(w http.ResponseWriter, r *http.Request) {
	var rd requestData
	var err error
	return func(w http.ResponseWriter, r *http.Request) {
		rd, err = parseRequest(r, pathPrefix)
		if err != nil {
			log.Println("REST error parsing request:", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods: ", "GET, HEAD, PUT, POST, DELETE, OPTIONS")

		switch r.Method {
		case "GET":
			log.Println("GET", r.URL.Path)
			handleGet(c, rd, w)
		case "OPTIONS":
			log.Println("OPTIONS", r.URL.Path)
			handleOptions(w, r)
		case "PUT":
			log.Println("PUT", r.URL.Path)
			handlePut(c, rd, w)
		case "POST":
			log.Println("POST", r.URL.Path)
			handlePost(c, rd, w)
		case "DELETE":
			log.Println("DELETE", r.URL.Path)
			handleDelete(c, rd, w)
		default:
			log.Println(r.Method, "Not allowed")
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		}
	}
}

func parseRequest(r *http.Request, pathPrefix string) (requestData, error) {
	var objStr = r.URL.Path
	var rd = requestData{}
	var oParts []string
	var rKey string

	if pathPrefix[0] != '/' {
		pathPrefix = "/" + pathPrefix
	}
	//check if there is a path
	objStr = strings.Replace(objStr, pathPrefix, "", 1)
	if len(objStr) == 0 || objStr == "/" {
		return requestData{}, errors.New("Invalid path")
	}
	//remova leading and trailing slashes
	if objStr[0] == '/' {
		objStr = objStr[1:]
	}
	if objStr[len(objStr)-1] == '/' {
		objStr = objStr[:len(objStr)-1]
	}
	//split path string
	oParts = strings.Split(objStr, "/")
	//get values from url
	if len(oParts) > 0 {
		rd.SchemaName = db.Escape(oParts[0])
	}
	if len(oParts) > 1 {
		rd.TableName = db.Escape(oParts[1])
	}
	if len(oParts) > 2 {
		rKey = db.Escape(strings.Join(oParts[2:], "/"))
		if len(rKey) > 2 && rKey[:2] == "\\\"" && rKey[len(rKey)-2:] == "\\\"" {
			rKey = rKey[2 : len(rKey)-2]
		}
		if rKey[:1] == "\"" && rKey[len(rKey)-1:] == "\"" {
			rKey = rKey[1 : len(rKey)-1]
		}
		rd.PrimaryKeyValues = strings.Split(rKey, ":")
		for i, val := range rd.PrimaryKeyValues {
			rd.PrimaryKeyValues[i] = db.Escape(val)
		}
	}
	if query, ok := r.URL.Query()["q"]; ok != false {
		if query[0][:1] == "{" {
			q := make(map[string]string)
			err := json.Unmarshal([]byte(query[0]), &q)
			if err != nil {
				return requestData{}, err
			}
			rd.Query = make(map[string]string)
			for k, v := range q {
				rd.Query[db.Escape(k)] = db.Escape(v)
			}
		}
	}
	//get form values
	err := r.ParseForm()
	if err == nil {
		rd.FormData = make(map[string]string)
		for k, v := range r.Form {
			rd.FormData[db.Escape(k)] = db.Escape(strings.Join(v, ""))
		}
	}
	return rd, nil
}

//optionsHandler needed for Access-Control-Allow-Origin
func handleOptions(w http.ResponseWriter, r *http.Request) {
	var allow string
	if headers, ok := r.Header["Access-Control-Request-Headers"]; ok == true {
		for _, header := range headers {
			if len(allow) > 0 {
				allow += ", "
			}
			allow += header
		}
	}
	if len(allow) > 0 {
		w.Header().Set("Access-Control-Allow-Headers", allow)
	}
}

func handleGet(c db.Conn, rd requestData, w http.ResponseWriter) {
	if rd.SchemaName != "" && rd.TableName != "" {
		if len(rd.PrimaryKeyValues) != 0 {
			//get by primary key
			cols, err := c.GetColumns(rd.SchemaName, rd.TableName)
			if err != nil {
				http.Error(w, "Not found", http.StatusNotFound)
				log.Println("REST error:", err)
				return
			}
			err = rd.setPrimaryKeyValues(cols)
			if err != nil {
				http.Error(w, "Not found", http.StatusNotFound)
				log.Println("REST error:", err)
				return
			}
			q, err := c.SelectSQL(rd.SchemaName, rd.TableName, cols)
			if err != nil {
				http.Error(w, "Not found", http.StatusNotFound)
				log.Println("REST error:", err)
				return
			}
			err = ServeQuery(c, q, w)
			if err != nil {
				http.Error(w, "Not found", http.StatusNotFound)
				log.Println("REST error:", err)
				return
			}
		} else if rd.Query != nil {
			//perform query
			q, err := c.QuerySQL(rd.SchemaName, rd.TableName, rd.Query)
			if err != nil {
				http.Error(w, "Not found", http.StatusNotFound)
				log.Println("REST error:", err)
				return
			}
			err = ServeQuery(c, q, w)
			if err != nil {
				http.Error(w, "Not found", http.StatusNotFound)
				log.Println("REST error:", err)
				return
			}
		} else {
			//return all rows in table
			q, err := c.SelectSQL(rd.SchemaName, rd.TableName, nil)
			if err != nil {
				http.Error(w, "Not found", http.StatusNotFound)
				log.Println("REST error:", err)
				return
			}
			err = ServeQuery(c, q, w)
			if err != nil {
				http.Error(w, "Not found", http.StatusNotFound)
				log.Println("REST error:", err)
				return
			}
		}
	} else {
		http.Error(w, "Not found", http.StatusNotFound)
		log.Println("REST error: invalid url")
		return
	}
}

func handlePut(c db.Conn, rd requestData, w http.ResponseWriter) {
	if rd.SchemaName != "" && rd.TableName != "" {
		cols, err := c.GetColumns(rd.SchemaName, rd.TableName)
		if err != nil {
			http.Error(w, "Not found", http.StatusNotFound)
			log.Println("REST error:", err)
			return
		}
		err = rd.setColValues(cols)
		if err != nil {
			http.Error(w, "Not found", http.StatusNotFound)
			log.Println("REST error:", err)
			return
		}
		query, err := c.InsertSQL(rd.SchemaName, rd.TableName, cols)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			log.Println("REST error:", err)
			return
		}
		id, n, err := c.Execute(query)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			log.Println("REST error:", err)
			return
		}
		err = ServeExecuteResult(id, n, w)
		if err != nil {
			log.Println("REST error:", err)
		}
		deleteFromQueryCache(rd.SchemaName, rd.TableName)
	} else {
		http.Error(w, "Not found", http.StatusNotFound)
		log.Println("REST error: invalid url")
		return
	}
}

func handlePost(c db.Conn, rd requestData, w http.ResponseWriter) {
	if rd.SchemaName != "" && rd.TableName != "" && len(rd.PrimaryKeyValues) > 0 {
		cols, err := c.GetColumns(rd.SchemaName, rd.TableName)
		if err != nil {
			http.Error(w, "Not found", http.StatusNotFound)
			log.Println("REST error:", err)
			return
		}
		err = rd.setColValues(cols)
		if err != nil {
			http.Error(w, "Not found", http.StatusNotFound)
			log.Println("REST error:", err)
			return
		}
		if len(rd.PrimaryKeyValues) > 0 {
			err = rd.setPrimaryKeyValues(cols)
			if err != nil {
				http.Error(w, "Not found", http.StatusNotFound)
				log.Println("REST error:", err)
				return
			}
		}
		query, err := c.UpdateSQL(rd.SchemaName, rd.TableName, cols)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			log.Println("REST error:", err)
			return
		}
		_, n, err := c.Execute(query)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			log.Println("REST error:", err)
			return
		}
		if n == 0 {
			http.Error(w, "Not found", http.StatusNotFound)
			log.Println("REST error: not found: " + rd.SchemaName + "/" + rd.TableName + "/" + strings.Join(rd.PrimaryKeyValues, ":"))
			return
		}
		err = ServeExecuteResult(-1, n, w)
		if err != nil {
			log.Println("REST error:", err)
		}
		deleteFromQueryCache(rd.SchemaName, rd.TableName)
	} else {
		http.Error(w, "Not found", http.StatusNotFound)
		log.Println("REST error: invalid url")
		return
	}
}

func handleDelete(c db.Conn, rd requestData, w http.ResponseWriter) {
	if rd.SchemaName != "" && rd.TableName != "" && len(rd.PrimaryKeyValues) != 0 {
		cols, err := c.GetColumns(rd.SchemaName, rd.TableName)
		if err != nil {
			http.Error(w, "Not found", http.StatusNotFound)
			log.Println("REST error:", err)
			return
		}
		err = rd.setPrimaryKeyValues(cols)
		if err != nil {
			http.Error(w, "Not found", http.StatusNotFound)
			log.Println("REST error:", err)
			return
		}
		q, err := c.DeleteSQL(rd.SchemaName, rd.TableName, cols)
		if err != nil {
			http.Error(w, "Not found", http.StatusNotFound)
			log.Println("REST error:", err)
			return
		}
		// err = ServeQuery(c, q, w)
		_, n, err := c.Execute(q)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			log.Println("REST error:", err)
			return
		}
		if n == 0 {
			http.Error(w, "Not found", http.StatusNotFound)
			log.Println("REST error: not found: " + rd.SchemaName + "/" + rd.TableName + "/" + strings.Join(rd.PrimaryKeyValues, ":"))
			return
		}
		err = ServeExecuteResult(-1, n, w)
		if err != nil {
			log.Println("REST error:", err)
		}
		deleteFromQueryCache(rd.SchemaName, rd.TableName)
	} else {
		http.Error(w, "Not found", http.StatusNotFound)
		log.Println("REST error: invalid url")
		return
	}
}
