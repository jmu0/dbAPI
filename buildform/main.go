package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/jmu0/dbAPI/db"
	"github.com/jmu0/dbAPI/db/mysql"
	"github.com/jmu0/dbAPI/db/postgresql"
)

var conn *sql.DB
var err error
var database, table string
var tables, dbs []string
var cols []db.Column

func main() {
	if len(os.Args) == 1 {
		fmt.Println("invalid args (html/template)")
		return
	}
	switch os.Args[1] {
	case "html":
		var html = cols2form(getCols(connect()))
		fmt.Print("Filename (" + strings.ToLower(table) + ".html)?")
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
		var html = cols2template(getCols(connect()))
		fmt.Print("Filename (" + strings.ToLower(table) + ".html)?")
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
		fmt.Println("invalid args (html/template)")
	}
}

func connect() db.Conn {
	var err error
	var tp, host, user, pwd string
	fmt.Print("Dababase type (mysql, postgresql): ")
	fmt.Scanln(&tp)
	fmt.Print("Host: ")
	fmt.Scanln(&host)
	fmt.Print("User: ")
	fmt.Scanln(&user)
	fmt.Print("Password: ")
	fmt.Scanln(&pwd)
	if tp == "mysql" {
		c := mysql.Conn{}
		err = c.Connect(map[string]string{
			"hostname": host,
			"username": user,
			"password": pwd,
		})
		if err != nil {
			panic(err)
		}
		return &c
	} else if tp == "postgresql" {
		var database string
		fmt.Print("Database: ")
		fmt.Scanln(&database)
		c := postgresql.Conn{}
		err = c.Connect(map[string]string{
			"hostname": host,
			"username": user,
			"password": pwd,
			"database": database,
		})
		if err != nil {
			panic(err)
		}
		return &c
	} else {
		panic("invalid type: " + tp)
	}
}

func getCols(c db.Conn) []db.Column {
	var err error
	var ret []db.Column
	dbs, err = c.GetSchemaNames()
	if err != nil {
		panic(err)
	}
	for _, db := range dbs {
		fmt.Println(db)
	}
	fmt.Print("Schema: ")
	fmt.Scanln(&database)
	tables, err = c.GetTableNames(database)
	if err != nil {
		panic(err)
	}
	for _, tbl := range tables {
		fmt.Println(tbl)
	}
	fmt.Print("Table: ")
	fmt.Scanln(&table)
	ret, err = c.GetColumns(database, table)
	if err != nil {
		panic(err)
	}
	return ret
}
