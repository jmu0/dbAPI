package main

import (
	"log"
	"net/http"

	"github.com/jmu0/dbAPI/db/mysqlAPI"
)

var listenAddr = ":8282"

func main() {
	mx := http.NewServeMux()
	mx.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		log.Println("Handle /")
		w.Write([]byte("Test!"))
	})
	mx.HandleFunc("/data/", func(w http.ResponseWriter, r *http.Request) {
		log.Println("Handle /data")
		mysqlAPI.HandleREST("/data", w, r)
	})

	log.Println("Listening on port", listenAddr)
	log.Fatal(http.ListenAndServe(listenAddr, mx))
}
