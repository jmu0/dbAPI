package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/jmu0/dbAPI/api"
	"github.com/jmu0/dbAPI/db"

	"github.com/jmu0/dbAPI/db/mysql"
	"github.com/jmu0/dbAPI/db/postgresql"
)

var listenAddr = ":8282"

func main() {
	// testMysql()
	// testPostgres()
	runAPIServer()
	// testGraphql()
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
	// fmt.Println("\nSchema names in database:")
	// fmt.Println(pg.GetSchemaNames())
	// fmt.Println("\nTables in public:")
	// fmt.Println(pg.GetTableNames("public"))
	fmt.Println("\nGet columns for tbl1:")
	cols, err := pg.GetColumns("public", "tbl1")
	printdbcols(cols)
	// c, err := pg.GetColumns("assortiment", "plant")
	// printdbcols(c)
	// fmt.Println("\nRelationships for assortiment.artikel:")
	// fmt.Println(pg.GetRelationships("assortiment", "artikel"))
}
func testMysql() {
	var d = mysql.Conn{}
	err := d.Connect(map[string]string{
		"hostname": "jos-desktop",
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
	fmt.Println("\nColumns in Verkoop.Orderregels:")
	fmt.Println(d.GetColumns("Verkoop", "Orderregels"))
	// c, _ := d.GetColumns("Assortiment", "Plant")
	// printdbcols(c)
	fmt.Println("\nRelationships for Assortiment.Plant:")
	// fmt.Println(d.GetRelationships("Assortiment", "Plant"))
	// fmt.Println("\nRelationships for Assortiment.Artikel:")
	// fmt.Println(d.GetRelationships("Assortiment", "Artikel"))
	// fmt.Println("\nRelationships for Assortiment.Voorraad:")
	// fmt.Println(d.GetRelationships("Assortiment", "Voorraad"))
}

func testGraphql() {
	mx := http.NewServeMux()
	mx.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Test!"))
	})
	// var d = postgresql.Conn{}
	// err := d.Connect(map[string]string{
	// 	"hostname": "jos-desktop",
	// 	"username": "jos",
	// 	"password": "jmu0!",
	// 	"database": "test",
	// })
	var d = mysql.Conn{}
	err := d.Connect(map[string]string{
		"hostname": "jos-desktop",
		"username": "web",
		"password": "jmu0!",
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	schema, err := api.BuildSchema(api.BuildSchemaArgs{
		Tables: []string{},
	}, &d)

	if err != nil {
		fmt.Println("Schema error:", err)
	}
	mx.HandleFunc("/graphql", func(w http.ResponseWriter, r *http.Request) {
		api.HandleGQL(&schema, w, r)
	})

	fmt.Println("Listening on port", listenAddr)
	log.Fatal(http.ListenAndServe(listenAddr, mx))

}

func printdbcols(cols []db.Column) {
	for _, c := range cols {
		fmt.Println("\nname:", c.Name)
		fmt.Println("type:", c.Type)
		fmt.Println("length:", c.Length)
		fmt.Println("nullable:", c.Nullable)
		fmt.Println("primary key:", c.PrimaryKey)
		fmt.Println("auto increment:", c.AutoIncrement)
		fmt.Println("default value:", c.DefaultValue)
		fmt.Println("value:", c.Value)
	}
}
func runAPIServer() {
	port := ":9999"
	mx := http.NewServeMux()
	// c := mysql.Conn{}
	// log.Println(c.Connect(map[string]string{
	// 	"hostname": "jos-desktop",
	// 	"username": "web",
	// 	"password": "jmu0!",
	// }))
	c := postgresql.Conn{}
	log.Println(c.Connect(map[string]string{
		"hostname": "localhost",
		"username": "jos",
		"password": "jmu0!",
		"database": "test",
	}))
	mx.HandleFunc("/data/", api.RestHandler("/data", &c))

	log.Println("Listening on port", port)
	log.Fatal(http.ListenAndServe(port, mx))
}
