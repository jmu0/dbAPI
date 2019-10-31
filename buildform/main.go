package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/jmu0/dbAPI/db/mysql"
)

var conn *sql.DB
var err error
var db, table string
var tables, dbs []string
var cols []mysql.Column

func main() {
	if len(os.Args) == 1 {
		fmt.Println("invalid args")
		return
	}
	switch os.Args[1] {
	case "html":
		var html = cols2form(getCols())
		fmt.Print("Bestandsnaam (" + strings.ToLower(table) + ".html)?")
		var filename string
		fmt.Scanln(&filename)
		if len(filename) == 0 {
			filename = strings.ToLower(table) + ".html"
		}
		err := ioutil.WriteFile(filename, []byte(html), 0770)
		if err != nil {
			fmt.Println("ERROR:", err)
		}
	case "template":
		var html = cols2template(getCols())
		fmt.Print("Bestandsnaam (" + strings.ToLower(table) + ".html)?")
		var filename string
		fmt.Scanln(&filename)
		if len(filename) == 0 {
			filename = strings.ToLower(table) + ".html"
		}
		err := ioutil.WriteFile(filename, []byte(html), 0770)
		if err != nil {
			fmt.Println("ERROR:", err)
		}
	default:
		fmt.Println("invalid args")
	}
}

func getCols() []mysql.Column {
	conn, err = mysql.Connect(map[string]string{
		"hostname": "database",
		"username": "web",
		"password": "jmu0!",
	})
	if err != nil {
		log.Fatal(err)
	}
	dbs = mysql.GetDatabaseNames(conn)
	for _, db := range dbs {
		fmt.Println(db)
	}
	fmt.Print("Database: ")
	fmt.Scanln(&db)
	tables = mysql.GetTableNames(conn, db)
	for _, tbl := range tables {
		fmt.Println(tbl)
	}
	fmt.Print("Table: ")
	fmt.Scanln(&table)
	return mysql.GetColumns(conn, db, table)
}
