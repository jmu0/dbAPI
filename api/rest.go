package api

// import (
// 	"encoding/json"
// 	"fmt"
// 	"log"
// 	"net/http"
// 	"strconv"
// 	"strings"
// )

// //HandleREST handle REST api for DbObject
// func HandleREST(pathPrefix string, w http.ResponseWriter, r *http.Request) string {
// 	//TODO: compress results
// 	var objStr = r.URL.Path
// 	db, err := Connect()
// 	if err != nil {
// 		http.Error(w, "REST: Could not connect to database", http.StatusInternalServerError)
// 		return ""
// 	}
// 	if pathPrefix[0] != '/' {
// 		pathPrefix = "/" + pathPrefix
// 	}
// 	objStr = strings.Replace(objStr, pathPrefix, "", 1)
// 	if len(objStr) == 0 {
// 		http.NotFound(w, r)
// 		return ""
// 	}
// 	if objStr[0] == '/' {
// 		objStr = objStr[1:]
// 	}
// 	if objStr[len(objStr)-1] == '/' {
// 		objStr = objStr[:len(objStr)-1]
// 	}
// 	//fmt.Println("REST DEBUG: objStr:", objStr)
// 	oParts := strings.Split(objStr, "/")
// 	var rDB, rTBL, rKey string
// 	if len(oParts) > 0 {
// 		rDB = Escape(oParts[0])
// 	}
// 	if len(oParts) > 1 {
// 		rTBL = Escape(oParts[1])
// 	}
// 	if len(oParts) > 2 {
// 		//KEYS WITH / USE "
// 		rKey = Escape(strings.Join(oParts[2:], "/"))
// 		if rKey[:2] == "\\\"" && rKey[len(rKey)-2:] == "\\\"" {
// 			rKey = rKey[2 : len(rKey)-2]
// 		}
// 		if rKey[:1] == "\"" && rKey[len(rKey)-1:] == "\"" {
// 			rKey = rKey[1 : len(rKey)-1]
// 		}
// 	}
// 	// log.Println("DEBUG rDB:", rDB, "rTBL:", rTBL, "rKey:", rKey)

// 	switch len(oParts) {
// 	case 1: //only db, write list of tables
// 		if r.Method == "GET" {
// 			// tbls := GetTableNames(db, objParts[0])
// 			tbls := GetTableNames(db, rDB)
// 			if len(tbls) > 0 {
// 				bytes, err := json.Marshal(tbls)
// 				if err != nil {
// 					fmt.Println("HandleRest: error encoding json:", err)
// 					http.Error(w, "Could not encode json", http.StatusInternalServerError)
// 					return ""
// 				}
// 				w.Header().Set("Content-Type", "application/json; charset=utf-8")
// 				w.Write(bytes)
// 			} else {
// 				http.Error(w, "Database doesn't exist", http.StatusNotFound)
// 				return ""
// 			}
// 		} else {
// 			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
// 			return ""
// 		}
// 	case 2: //table, query rows
// 		if r.Method == "GET" {
// 			// q := "select * from " + objParts[0] + "." + objParts[1]
// 			q := "select * from " + rDB + "." + rTBL
// 			//TODO check for query
// 			if where, ok := r.URL.Query()["q"]; ok != false {
// 				q += " where " + Escape(where[0])
// 				q = strings.Replace(q, "''", "'", -1)
// 			}
// 			// log.Println("DEBUG: REST query:", q)
// 			writeQueryResults(db, q, w)
// 		} else if r.Method == "POST" { //post to a db table url
// 			// cols := getColsWithValues(db, objParts[0], objParts[1], r)
// 			cols := getColsWithValues(db, rDB, rTBL, r)
// 			if len(cols) == 0 {
// 				http.Error(w, "REST: Object not found", http.StatusNotFound)
// 				return ""
// 			}
// 			log.Println("POST:", r.URL.Path)
// 			// n, id, err := save(objParts[0], objParts[1], cols)
// 			n, id, err := save(rDB, rTBL, cols)
// 			if err != nil {
// 				log.Println("REST ERROR: POST:", oParts, err)
// 				http.Error(w, "Could not save", http.StatusInternalServerError)
// 				return ""
// 			}
// 			if n == 1 && id > -1 {
// 				cols = setAutoIncColumn(id, cols)
// 			}
// 			w.Header().Set("Content-Type", "application/json; charset=utf-8")
// 			w.Write([]byte("{\"n\":\"" + strconv.Itoa(n) + "\",\"id\":\"" + strconv.Itoa(id) + "\"}"))
// 			// json, err := cols2json(objParts[1], cols)
// 			json, err := cols2json(rTBL, cols)
// 			if err != nil {
// 				return ""
// 			}
// 			return string(json)
// 		} else {
// 			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
// 			return ""
// 		}
// 		return ""
// 	default: //table primary key, perform CRUD
// 		//ERROR does not work when key contains backslash: case 3: //table primary key, perform CRUD
// 		// fmt.Println("DEBUG: HandleRest:", cols)
// 		switch r.Method {
// 		case "GET":
// 			log.Println("REST: GET:", oParts)
// 			// cols := getColsWithValues(db, objParts[0], objParts[1], r)
// 			cols := getColsWithValues(db, rDB, rTBL, r)
// 			//put primary key values in columns
// 			// keys := strings.Split(strings.Join(objParts[2:], "/"), ":")
// 			keys := strings.Split(rKey, ":")
// 			keyCounter := 0
// 			for index, column := range cols {
// 				if column.Key == "PRI" {
// 					cols[index].Value = Escape(keys[keyCounter])
// 					keyCounter++
// 					if keyCounter == len(keys) {
// 						break
// 					}
// 				}
// 			}
// 			// q := "select * from " + objParts[0] + "." + objParts[1] + " where "
// 			q := "select * from " + rDB + "." + rTBL + " where "
// 			where, err := StrPrimaryKeyWhereSQL(cols)
// 			if err != nil {
// 				http.Error(w, "Could not build query", http.StatusInternalServerError)
// 				return ""
// 			}
// 			q += where
// 			//log.Println("REST: GET: query ", q)
// 			writeQueryResults(db, q, w)
// 		case "POST": //post to a object id
// 			// cols := getColsWithValues(db, objParts[0], objParts[1], r)
// 			cols := getColsWithValues(db, rDB, rTBL, r)
// 			if len(cols) == 0 {
// 				http.Error(w, "Object not found", http.StatusNotFound)
// 				return ""
// 			}
// 			//put primary key values in columns
// 			// keys := strings.Split(strings.Join(objParts[2:], "/"), ":")
// 			keys := strings.Split(rKey, ":")
// 			keyCounter := 0
// 			for index, column := range cols {
// 				if column.Key == "PRI" {
// 					cols[index].Value = Escape(keys[keyCounter])
// 					keyCounter++
// 					if keyCounter == len(keys) {
// 						break
// 					}
// 				}
// 			}
// 			log.Println("POST:", r.URL.Path)
// 			// log.Println("DEBUG:POST:", rDB, rTBL, keys)
// 			// log.Println("DEBUG POST:", cols)
// 			// n, id, err := save(objParts[0], objParts[1], cols)
// 			n, id, err := save(rDB, rTBL, cols)
// 			if err != nil {
// 				log.Println("REST: ERROR: POST:", oParts, err)
// 				http.Error(w, "Could not save", http.StatusInternalServerError)
// 				return ""
// 			}
// 			w.Header().Set("Content-Type", "application/json; charset=utf-8")
// 			w.Write([]byte("{\"n\":\"" + strconv.Itoa(n) + "\",\"id\":\"" + strconv.Itoa(id) + "\"}"))
// 			// json, err := cols2json(objParts[1], cols)
// 			json, err := cols2json(rTBL, cols)
// 			if err != nil {
// 				return ""
// 			}
// 			return string(json)
// 		case "DELETE":
// 			// cols := getColsWithValues(db, objParts[0], objParts[1], r)
// 			cols := getColsWithValues(db, rDB, rTBL, r)
// 			if len(cols) == 0 {
// 				http.Error(w, "Object not found", http.StatusNotFound)
// 				return ""
// 			}
// 			//put primary key values in columns
// 			// keys := strings.Split(strings.Join(objParts[2:], "/"), ":")
// 			keys := strings.Split(rKey, ":")
// 			keyCounter := 0
// 			for index, column := range cols {
// 				if column.Key == "PRI" {
// 					cols[index].Value = Escape(keys[keyCounter])
// 					keyCounter++
// 					if keyCounter == len(keys) {
// 						break
// 					}
// 				}
// 			}
// 			log.Println("REST: DELETE:", oParts)
// 			// n, err := delete(objParts[0], objParts[1], cols)
// 			n, err := delete(rDB, rTBL, cols)
// 			if err != nil {
// 				log.Println("REST: ERROR: POST:", oParts, err)
// 				http.Error(w, "Could not save", http.StatusInternalServerError)
// 				return ""
// 			}
// 			w.Header().Set("Content-Type", "application/json; charset=utf-8")
// 			w.Write([]byte("{\"n\":\"" + strconv.Itoa(n) + "\"}"))
// 			return string(n)
// 		default:
// 			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
// 			return ""
// 		}
// 		// default:
// 		// 	http.Error(w, "Invalid Path", http.StatusInternalServerError)
// 		// 	return ""
// 	}
// 	return ""
// }
