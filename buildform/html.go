package main

import (
	"strings"

	"github.com/jmu0/dbAPI/db"
)

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
