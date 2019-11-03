package api

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/jmu0/dbAPI/db"
)

//ServeQuery does query and writes json to responseWriter
func ServeQuery(con db.Conn, query string, w http.ResponseWriter) error {
	var ret interface{}
	//get query results
	result, err := db.Query(con, query)
	if err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return err
	}

	//return array or single object
	if len(result) == 1 {
		ret = result[0]
	} else {
		ret = result
	}
	//get json
	bytes, err := json.Marshal(ret)
	if err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return err
	}
	//drop password fields
	var pwReg = ",\"?([P,p]ass[W,w]o?r?d|[W,w]acht[W,w]o?o?r?d?)\"?:\"(.*?)\""
	passwdReg := regexp.MustCompile(pwReg)
	bytes = []byte(passwdReg.ReplaceAllString(string(bytes), ""))
	//compress result
	bytes, err = compress(bytes)
	if err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return err
	}
	//write result
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Encoding", "gzip")
	w.Write(bytes)
	return nil
}

//ServeExecuteResult returns result for a query that doesn't return rows
func ServeExecuteResult(rowsAffected int64, w http.ResponseWriter) error {
	bytes, err := json.Marshal(map[string]string{
		"n": strconv.FormatInt(rowsAffected, 10),
	})
	if err != nil {
		http.Error(w, "", http.StatusInternalServerError)
		return err
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write(bytes)
	return nil
}

//compress returns gzipped []byte
func compress(inp []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, err := zw.Write(inp)
	if err != nil {
		return inp, err
	}
	if err := zw.Close(); err != nil {
		return inp, err
	}
	return []byte(buf.String()), nil
}

func getColsWithValues(c db.Conn, dbName string, tblName string, r *http.Request) []db.Column {
	//TODO: delet this function?
	cols, err := c.GetColumns(dbName, tblName)
	data, err := getRequestData(r)
	if err != nil {
		log.Println("REST: ERROR: POST:", dbName, tblName, err)
	}

	//set column values
	values2columns(&cols, data)
	return cols
}

func values2columns(cols *[]db.Column, values map[string]interface{}) {
	//TODO: used in more than getColsWithValues?
	for key, value := range values {
		index := findColIndex(key, *cols)
		if index > -1 {
			(*cols)[index].Value = db.Escape(value.(string))
		}
	}
}

func findColIndex(field string, cols []db.Column) int {
	//TODO: delete this function?
	for index, col := range cols {
		if strings.ToLower(col.Name) == strings.ToLower(field) {
			return index
		}
	}
	return -1
}

func cols2json(table string, cols []db.Column) ([]byte, error) {
	//TODO: delete this function?
	var ret map[string]interface{}
	ret = make(map[string]interface{})
	ret["type"] = table
	for _, col := range cols {
		ret[col.Name] = col.Value
	}
	json, err := json.Marshal(ret)
	if err != nil {
		return []byte(""), err
	}
	return json, nil
}

//getRequestData get data from post request
func getRequestData(req *http.Request) (map[string]interface{}, error) {
	//TODO: delete this function?
	err := req.ParseForm()
	if err != nil {
		return make(map[string]interface{}), err
	}
	res := make(map[string]interface{})
	for k, v := range req.Form {
		res[k] = strings.Join(v, "")
	}
	return res, nil
}

func setAutoIncColumn(id int, cols []db.Column) []db.Column {
	//TODO: delete this function?
	//fmt.Println("DEBUG:setAutoIncColumn")
	for index, col := range cols {
		//TODO: put AutoIncrement boolean in column type
		if strings.Contains(col.Type, "int") && col.PrimaryKey == true {
			//fmt.Println("DEBUG:found", col.Field)
			cols[index].Value = id
		}
	}
	return cols
}
