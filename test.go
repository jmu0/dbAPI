package main

import (
	"log"
	"net/http"

	"github.com/jmu0/dbAPI/db/mysql"
)

var listenAddr = ":8282"

func main() {
	mx := http.NewServeMux()
	mx.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Test!"))
	})

	//*
	mx.HandleFunc("/data/", func(w http.ResponseWriter, r *http.Request) {
		mysql.HandleREST("/data", w, r)
	})
	//*/

	// schema, err := mysql.TestSchema()
	schema, err := mysql.BuildSchema(mysql.BuildSchemaArgs{
		Tables: []string{
			"Assortiment.Artikel",
			"Assortiment.Maat",
			"Assortiment.Plant",
			"Assortiment.Categorie",
			"Assortiment.Voorraad",
			"Assortiment.Prijslijst",
		},
		// Tables: []string{"Assortiment.Plant"},
	})

	if err != nil {
		log.Println("Schema error:", err)
	}
	mx.HandleFunc("/graphql", func(w http.ResponseWriter, r *http.Request) {
		mysql.HandleGQL(&schema, w, r)
	})

	log.Println("Listening on port", listenAddr)
	log.Fatal(http.ListenAndServe(listenAddr, mx))
}
