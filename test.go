package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/jmu0/dbAPI/api"
	"github.com/jmu0/dbAPI/db/mysql"
	"github.com/jmu0/dbAPI/db/postgresql"
)

var listenAddr = ":8282"

func main() {
	testMysql()
	testPostgres()
}

func testPostgres() {
	var pg = postgresql.Conn{}
	err := pg.Connect(map[string]string{
		"database": "test",
		"hostname": "localhost",
		"username": "jos",
		"password": "jmu0!",
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("\nSchema names in database:")
	fmt.Println(pg.GetSchemaNames())
	fmt.Println("\nTables in assortiment:")
	fmt.Println(pg.GetTableNames("Assortiment"))
}
func testMysql() {
	var d = mysql.Conn{}
	err := d.Connect(map[string]string{
		"hostname": "database",
		"username": "web",
		"password": "jmu0!",
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("\nDatabases on server:")
	fmt.Println(d.GetSchemaNames())
	fmt.Println("\nTables in assortiment:")
	fmt.Println(d.GetTableNames("Assortiment"))
}

func testGraphql() {
	mx := http.NewServeMux()
	mx.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Test!"))
	})

	//*
	mx.HandleFunc("/data/", func(w http.ResponseWriter, r *http.Request) {
		api.HandleREST("/data", w, r)
	})
	//*/

	// schema, err := mysql.TestSchema()
	schema, err := api.BuildSchema(api.BuildSchemaArgs{
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
		fmt.Println("Schema error:", err)
	}
	mx.HandleFunc("/graphql", func(w http.ResponseWriter, r *http.Request) {
		api.HandleGQL(&schema, w, r)
	})

	fmt.Println("Listening on port", listenAddr)
	log.Fatal(http.ListenAndServe(listenAddr, mx))

}
