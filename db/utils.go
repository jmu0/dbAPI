package db

import (
	"database/sql"
	"log"
	"strconv"
	"strings"
)

//Execute executes query without returning results. returns (lastInsertId, rowsAffected, error)
func Execute(c Conn, query string) (int64, int64, error) {
	// fmt.Println(query)
	res, err := c.GetConnection().Exec(query)
	if err != nil {
		return 0, 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, 0, err
	}

	return id, n, nil
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

//SetIndexName sets index name if it is empty or if its the same as Columns
func SetIndexName(schemaName, tableName string, index *Index) {
	//force unique index names because differences postgres/mysql causes errors on duplicate names
	if len(index.Name) == 0 ||
		index.Name == index.Columns ||
		strings.Contains(index.Name, schemaName) == false ||
		strings.Contains(index.Name, tableName) == false {
		index.Name = schemaName + "_" + tableName + "_" + strings.Replace(strings.Replace(index.Columns, ", ", "_", -1), ",", "_", -1) + "_index"
	}
}

//SetForeignKeyName sets foreign key name if it is empty
func SetForeignKeyName(fk *ForeignKey) {
	if len(fk.Name) == 0 {
		fk.Name = strings.Replace(fk.ToTable, ".", "_", -1) + "_"
		fk.Name += strings.Replace(strings.Replace(fk.ToCols, ", ", "_", -1), ",", "_", -1) + "_fkey"
	}
}

// SortTablesByForeignKey sorts tables for building creat table SQL
func SortTablesByForeignKey(tbls []Table) {
	var i, j, k int
	var spl []string
	var swapped bool
	i = 0
	for i < len(tbls) {
		swapped = false
		for _, fk := range tbls[i].ForeignKeys {
			spl = strings.Split(fk.ToTable, ".")
			if len(spl) == 2 {
				for j = i + 1; j < len(tbls); j++ {
					if spl[0] == tbls[j].Schema && spl[1] == tbls[j].Name {
						for k = i; k < j; k++ {
							tbls[k], tbls[k+1] = tbls[k+1], tbls[k]
						}
						swapped = true
					}
				}
			}
		}
		if !swapped {
			i++
		}
	}

}
