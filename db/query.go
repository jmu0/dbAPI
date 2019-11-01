package db

import (
	"database/sql"
	"errors"
)

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

//Execute executes query without returning results. returns (lastInsertId, rowsAffected, error)
func Execute(c Conn, query string, params []interface{}) (int64, int64, error) {
	res, err := c.GetConnection().Exec(query, params)
	if err != nil {
		return 0, 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, 0, err
	}
	return id, n, nil
}

//SaveSQL builds SQL query and parameters for saving data
func SaveSQL(schemaName, tableName string, cols []Column) (string, []interface{}, error) {

	return "", make([]interface{}, 0), nil
}

//DeleteSQL builds SQL query for deleting data
func DeleteSQL(schemaName, tableName string, cols []Column) (string, error) {
	return "", nil
}

//primaryKeyWhereSQL returns where part (without 'where') of query
func primaryKeyWhereSQL(cols []Column) (string, error) {
	var ret string
	for _, c := range cols {
		if c.PrimaryKey == true {
			if len(ret) > 0 {
				ret += " and"
			}
			ret += " " + c.Name + " = " + interface2string(c.Value)
		}
	}
	if len(ret) == 0 {
		return "", errors.New("Primary key not found (StrPrimaryKeyWhereSQL")
	}
	return ret, nil
}

//primaryKeyCols filters primary key columns from []Column
func primaryKeyCols(cols []Column) []Column {
	var ret []Column
	for _, c := range cols {
		if c.PrimaryKey == true {
			ret = append(ret, c)
		}
	}
	return ret
}
