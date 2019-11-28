package db

import (
	"database/sql"
	"log"
	"sort"
	"strconv"
	"strings"
)

//Execute executes query without returning results. returns (lastInsertId, rowsAffected, error)
func Execute(c Conn, query string) (int64, error) {
	// fmt.Println(query)
	res, err := c.GetConnection().Exec(query)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	return n, nil
}

//Query queries the database
func Query(c Conn, query string) ([]map[string]interface{}, error) {
	res := make([]map[string]interface{}, 0)
	rows, err := c.GetConnection().Query(query)
	if err != nil {
		return res, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		rows.Close()
		return res, err
	}
	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		rows.Scan(scanArgs...)
		v := make(map[string]interface{})
		var value interface{}
		for i, col := range values {
			if col == nil {
				value = ""
			} else {
				value = string(col)
			}
			v[columns[i]] = value
		}
		res = append(res, v)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return res, err
	}
	return res, nil
}

//Escape string to prevent common sql injection attacks
func Escape(str string) string {
	// ", ', 0=0
	str = strings.Replace(str, "\"", "\\\"", -1)
	str = strings.Replace(str, "''", "'", -1)
	str = strings.Replace(str, "'", "''", -1)

	// \x00, \n, \r, \ and \x1a"
	str = strings.Replace(str, "\x00", "", -1)
	str = strings.Replace(str, "\n", "", -1)
	str = strings.Replace(str, "\r", "", -1)
	str = strings.Replace(str, "\x1a", "", -1)

	//multiline attack
	str = strings.Replace(str, ";", " ", -1)

	//comments attack
	str = strings.Replace(str, "--", "", -1)
	str = strings.Replace(str, "#", "", -1)
	str = strings.Replace(str, "/*", "", -1)
	str = strings.Replace(str, "*/", "", -1)
	return str
}

//Interface2string returns escaped string for interface{}
func Interface2string(val interface{}, quote bool) string {
	var value string
	if val == nil {
		return ""
	}
	switch t := val.(type) {
	case string:
		if quote {
			value += "'" + Escape(val.(string)) + "'"

		} else {
			value += Escape(val.(string))
		}
	case int:
		value += strconv.Itoa(val.(int))
	case int32:
		value += strconv.FormatInt(int64(val.(int32)), 10)
	case int64:
		value += strconv.FormatInt(val.(int64), 10)
	case []uint8:
		if quote {
			value += "'" + string([]byte(val.([]uint8))) + "'"
		} else {
			value += string([]byte(val.([]uint8)))
		}
	default:
		log.Println("WARNING: type not handled:", t, "using string")
		if quote {
			value += "'" + Escape(val.(string)) + "'"
		} else {
			value += Escape(val.(string))
		}
	}
	return value
}

//HasSchema checks if schema exists in database
func HasSchema(schemaName string, c Conn) bool {
	lst, err := c.GetSchemaNames()
	if err != nil {
		return false
	}
	for _, item := range lst {
		if item == schemaName {
			return true
		}
	}
	return false
}

//HasTable checks if table exists in schema
func HasTable(schemaName, tableName string, c Conn) bool {
	schema, err := GetSchema(schemaName, c)
	if err != nil {
		return false
	}
	for _, tbl := range schema.Tables {
		if tbl.Name == tableName {
			return true
		}
	}
	return false
}

//DoubleQuote puts quotes around string, (schema.table) and (col, col)
func DoubleQuote(str string) string {
	var res, sep string
	var spl []string
	if strings.Contains(str, ",") {
		sep = ", "
		spl = strings.Split(str, ",")
	} else if strings.Contains(str, ".") {
		sep = "."
		spl = strings.Split(str, ".")
	} else {
		return "\"" + str + "\""
	}
	for _, item := range spl {
		if len(res) > 0 {
			res += sep
		}
		res += "\"" + strings.TrimSpace(item) + "\""
	}
	return res
}

// SortTablesByForeignKey sorts tables for building creat table SQL
func SortTablesByForeignKey(tbls []Table) {
	sort.SliceStable(tbls, func(i, j int) bool {
		var spl []string
		for _, fk := range tbls[i].ForeignKeys {
			spl = strings.Split(fk.ToTable, ".")
			if len(spl) == 2 {
				if spl[0] == tbls[j].Schema && spl[1] == tbls[j].Name {
					return false
				}
			}
		}
		return true
	})
}
