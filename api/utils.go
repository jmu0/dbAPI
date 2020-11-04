package api

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/jmu0/dbAPI/db"
	"github.com/jmu0/dbAPI/db/mysql"
	"github.com/jmu0/dbAPI/db/postgresql"
	"github.com/jmu0/settings"
)

//GetConnection connects to database from file/environment var./command line args.
func GetConnection(filename string) (db.Conn, error) {
	dbsettings := map[string]string{
		"driver":   "",
		"hostname": "",
		"username": "",
		"password": "",
		"database": "",
	}
	settings.Load(filename, &dbsettings)
	var conn db.Conn
	if dbsettings["driver"] == "mysql" {
		conn = &mysql.Conn{}
	} else if dbsettings["driver"] == "postgresql" {
		conn = &postgresql.Conn{}
	} else {
		return nil, errors.New("invalid database driver")
	}
	err = conn.Connect(dbsettings)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

//ServeQuery does query and writes json to responseWriter
func ServeQuery(con db.Conn, query string, w http.ResponseWriter) error {
	var ret interface{}
	//get query results
	result, err := con.Query(query)
	if err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return err
	}
	if len(result) == 0 {
		http.Error(w, "Not found", http.StatusNotFound)
		return errors.New("No records found")
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
func ServeExecuteResult(lastInsertID, rowsAffected int64, w http.ResponseWriter) error {
	bytes, err := json.Marshal(map[string]string{
		"id": strconv.FormatInt(lastInsertID, 10),
		"n":  strconv.FormatInt(rowsAffected, 10),
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

func findColIndex(field string, cols []db.Column) int {
	for index, col := range cols {
		if strings.ToLower(col.Name) == strings.ToLower(field) {
			return index
		}
	}
	return -1
}
