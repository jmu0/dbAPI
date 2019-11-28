package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/jmu0/dbAPI/db"
)

var database, table string
var tables, dbs []string

func handleHTML() {
	var html = cols2form(getCols(connect()))
	fmt.Println(html)
}

func handleTemplate() {
	var html = cols2template(getCols(connect()))
	fmt.Println(html)
}

func getCols(c db.Conn) []db.Column {
	var err error
	var ret []db.Column
	if _, ok := s["schema"]; !ok {
		for _, db := range dbs {
			fmt.Println(db)
		}
		dbs, err = c.GetSchemaNames()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Print("\nSchema: ")
		fmt.Scanln(&database)
	} else {
		database = s["schema"]
	}
	if _, ok := s["table"]; !ok {
		tables, err = c.GetTableNames(database)
		if err != nil {
			log.Fatal(err)
		}
		for _, tbl := range tables {
			fmt.Println(tbl)
		}
		fmt.Print("Table: ")
		fmt.Scanln(&table)
	} else {
		table = s["table"]
	}
	ret, err = c.GetColumns(database, table)
	if err != nil {
		log.Fatal(err)
	}
	return ret
}

func cols2form(cols []db.Column) string {
	var html, val string
	html = "<table class=\"properties\">\n"
	for _, col := range cols {
		val = ""
		if col.Value != nil {
			val = col.Value.(string)
		}
		html += "\t<tr>\n"
		html += "\t\t<td>" + col.Name + "</td>\n"
		html += "\t\t<td><input type=\""
		// fmt.Println(col)
		if strings.Contains(col.Type, "varchar") {
			html += "text"
		} else if strings.Contains(col.Type, "tinyint") {
			html += "checkbox"
		} else if strings.Contains(col.Type, "int") {
			html += "number"
		} else if col.Type == "datetime" {
			html += "date"
		} else {
			html += "text"
		}
		html += "\" name=\"" + col.Name + "\" data-key=\"" + strings.ToLower(col.Name) + "\"  value=\"" + val + "\" /></td>\n"
		html += "\t\t<td></td>\n"
		html += "\t</tr>\n"
	}
	html += "</table>"
	return html
}
func cols2template(cols []db.Column) string {
	var html string
	html = "<table class=\"properties\">\n"
	for _, col := range cols {
		html += "\t<tr>\n"
		html += "\t\t<td>" + col.Name + "</td>\n"
		html += "\t\t<td><input type=\""
		// fmt.Println(col)
		if strings.Contains(col.Type, "varchar") {
			html += "text"
		} else if strings.Contains(col.Type, "tinyint") {
			html += "checkbox"
		} else if strings.Contains(col.Type, "int") {
			html += "number"
		} else if col.Type == "datetime" {
			html += "date"
		} else {
			html += "text"
		}
		html += "\" name=\"" + col.Name + "\" data-key=\"" + strings.ToLower(col.Name) + "\" value=\"${{" + col.Name + "}}${{" + strings.ToLower(col.Name) + "}}\" /></td>\n"
		html += "\t</tr>\n"
	}
	html += "</table>"
	return html
}
