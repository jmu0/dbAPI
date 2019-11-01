package api

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"net/http"
	"regexp"

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
