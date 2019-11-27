package db

import (
	"errors"
)

//SelectSQL builds SQL query for selecting record by PrimaryKey
func SelectSQL(schemaName, tableName string, cols []Column) (string, error) {
	if cols == nil {
		return "select * from " + schemaName + "." + tableName, nil
	}
	q := "select * from " + schemaName + "." + tableName + " where "
	where, err := primaryKeyWhereSQL(cols)
	if err != nil {
		return "", errors.New("Could not build query: " + err.Error())
	}
	q += where
	return q, nil
}

//QuerySQL builds SQL query, construct WHERE statement from ESCAPED map[string]string
func QuerySQL(schemaName, tableName string, query map[string]string) (string, error) {
	if query == nil {
		return "", errors.New("No query")
	}
	var q = "select * from " + schemaName + "." + tableName + " where "
	var where = ""
	for k, v := range query {
		if len(where) > 0 {
			where += ", and "
		}
		if v[:2] == ">=" {
			where += k + " >= '" + v[2:] + "' "
		} else if v[:2] == "<=" {
			where += k + " <= '" + v[2:] + "' "
		} else if v[:1] == ">" {
			where += k + " > '" + v[1:] + "' "
		} else if v[:1] == "<" {
			where += k + " < '" + v[1:] + "' "
		} else if v[:1] == "*" && string(v[len(v)-1]) == "*" {
			where += k + " like '%" + v[1:len(v)-1] + "%' "
		} else if v[:1] == "*" {
			where += k + " like '%" + v[1:] + "' "
		} else if string(v[len(v)-1]) == "*" {
			where += k + " like '" + v[:len(v)-1] + "%' "
		} else {
			where += k + " = '" + v + "' "
		}
	}
	if len(where) == 0 {
		return "", errors.New("No query")
	}
	q += where
	return q, nil
}

//InsertSQL builds SQL query and parameters for inserting data
func InsertSQL(schemaName, tableName string, cols []Column) (string, error) {
	query := "insert into " + schemaName + "." + tableName + " "
	fields := "("
	strValues := "("
	for _, c := range cols {
		if c.Value != nil {
			if c.AutoIncrement == false {
				if len(fields) > 1 {
					fields += ", "
				}
				fields += c.Name
				if len(strValues) > 1 {
					strValues += ", "
				}
				strValues += Interface2string(c.Value, true)
			}
		}
	}
	if len(fields) == 1 {
		return "", errors.New("No columns contains a value")
	}
	fields += ")"
	strValues += ")"
	query += fields + " values " + strValues
	return query, nil
}

//UpdateSQL builds SQL query and parameters for updating data
func UpdateSQL(schemaName, tableName string, cols []Column) (string, error) {
	query := "update " + schemaName + "." + tableName + " set "
	fields := ""
	for _, c := range cols {
		if c.Value != nil {
			if c.AutoIncrement == false {
				if len(fields) > 0 {
					fields += ", "
				}
				fields += c.Name + "=" + Interface2string(c.Value, true)
			}
		}
	}
	if len(fields) == 0 {
		return "", errors.New("No columns contains a value")
	}
	query += fields + " where "
	where, err := primaryKeyWhereSQL(cols)
	if err != nil {
		return "", err
	}
	query += where
	return query, nil
}

//DeleteSQL builds SQL query for deleting data
func DeleteSQL(schemaName, tableName string, cols []Column) (string, error) {
	query := "delete from " + schemaName + "." + tableName + " where "
	where, err := primaryKeyWhereSQL(cols)
	if err != nil {
		return "", err
	}
	query += where
	return query, nil
}

//primaryKeyWhereSQL returns where part (without 'where') of query
func primaryKeyWhereSQL(cols []Column) (string, error) {
	var ret string
	for _, c := range cols {
		if c.PrimaryKey == true {
			if c.Value == nil {
				return "", errors.New("Primary key column " + c.Name + " has no value")
			}
			if len(ret) > 0 {
				ret += " and"
			}
			ret += " " + c.Name + " = " + Interface2string(c.Value, true)
		}
	}
	if len(ret) == 0 {
		return "", errors.New("Primary key not found in []Column")
	}
	return ret, nil
}
