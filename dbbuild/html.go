package main

import (
	"strings"

	"github.com/jmu0/dbAPI/db/mysql"
)

func cols2form(cols []mysql.Column) string {
	var html, val string
	html = "<table class=\"properties\">\n"
	for _, col := range cols {
		val = ""
		if col.Value != nil {
			val = col.Value.(string)
		}
		html += "\t<tr>\n"
		html += "\t\t<td>" + col.Field + "</td>\n"
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
		html += "\" name=\"" + col.Field + "\" data-key=\"" + col.Field + "\"  value=\"" + val + "\" /></td>\n"
		html += "\t\t<td></td>\n"
		html += "\t</tr>\n"
	}
	html += "</table>"
	return html
}
func cols2template(cols []mysql.Column) string {
	var html string
	html = "<table class=\"properties\">\n"
	for _, col := range cols {
		html += "\t<tr>\n"
		html += "\t\t<td>" + col.Field + "</td>\n"
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
		html += "\" name=\"" + col.Field + "\" data-key=\"" + col.Field + "\" value=\"${{" + col.Field + "}}\" /></td>\n"
		html += "\t</tr>\n"
	}
	html += "</table>"
	return html
}
