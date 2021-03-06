package postgresql

import (
	"errors"

	"github.com/jmu0/dbAPI/db"
)

//SelectSQL builds SQL query for selecting record by PrimaryKey
func (conn *Conn) SelectSQL(schemaName, tableName string, cols []db.Column) (string, error) {
	if cols == nil {
		return "select * from " + conn.Quote(schemaName+"."+tableName), nil
	}
	q := "select * from " + conn.Quote(schemaName+"."+tableName) + " where "
	where, err := primaryKeyWhereSQL(cols, conn)
	if err != nil {
		return "", errors.New("Could not build query: " + err.Error())
	}
	q += where
	return q, nil
}

//QuerySQL builds SQL query, construct WHERE statement from ESCAPED map[string]string
func (conn *Conn) QuerySQL(schemaName, tableName string, query map[string]string) (string, error) {
	if query == nil {
		return "", errors.New("No query")
	}
	var q = "select * from " + conn.Quote(schemaName+"."+tableName) + " where "
	var where = ""
	for k, v := range query {
		if len(where) > 0 {
			where += ", and "
		}
		if v[:2] == ">=" {
			where += conn.Quote(k) + " >= '" + v[2:] + "' "
		} else if v[:2] == "<=" {
			where += conn.Quote(k) + " <= '" + v[2:] + "' "
		} else if v[:1] == ">" {
			where += conn.Quote(k) + " > '" + v[1:] + "' "
		} else if v[:1] == "<" {
			where += conn.Quote(k) + " < '" + v[1:] + "' "
		} else if v[:1] == "*" && string(v[len(v)-1]) == "*" {
			where += conn.Quote(k) + " like '%" + v[1:len(v)-1] + "%' "
		} else if v[:1] == "*" {
			where += conn.Quote(k) + " like '%" + v[1:] + "' "
		} else if string(v[len(v)-1]) == "*" {
			where += conn.Quote(k) + " like '" + v[:len(v)-1] + "%' "
		} else {
			where += conn.Quote(k) + " = '" + v + "' "
		}
	}
	if len(where) == 0 {
		return "", errors.New("No query")
	}
	q += where
	return q, nil
}

//InsertSQL builds SQL query and parameters for inserting data
func (conn *Conn) InsertSQL(schemaName, tableName string, cols []db.Column) (string, error) {
	query := "insert into " + conn.Quote(schemaName+"."+tableName) + " "
	fields := "("
	strValues := "("
	var autoinc = false
	for _, c := range cols {
		if c.AutoIncrement == true {
			autoinc = true
		}
		if c.Value != nil {
			if c.AutoIncrement == false {
				if len(fields) > 1 {
					fields += ", "
				}
				fields += conn.Quote(c.Name)
				if len(strValues) > 1 {
					strValues += ", "
				}
				strValues += db.Interface2string(c.Value, true)
			}
		}
	}
	if len(fields) == 1 {
		return "", errors.New("No columns contains a value")
	}
	fields += ")"
	strValues += ")"
	query += fields + " values " + strValues
	if autoinc == true {
		returning := ""
		for _, c := range cols {
			if c.PrimaryKey == true && c.AutoIncrement == true {
				if len(returning) > 0 {
					returning += ", "
				}
				returning += conn.Quote(c.Name)
			}
		}
		if len(returning) > 0 {
			query += " returning " + returning
		}
	}
	return query, nil
}

//UpdateSQL builds SQL query and parameters for updating data
func (conn *Conn) UpdateSQL(schemaName, tableName string, cols []db.Column) (string, error) {
	query := "update " + conn.Quote(schemaName+"."+tableName) + " set "
	fields := ""
	for _, c := range cols {
		if c.Value != nil {
			if c.AutoIncrement == false && c.PrimaryKey == false {
				if len(fields) > 0 {
					fields += ", "
				}
				fields += conn.Quote(c.Name) + "=" + db.Interface2string(c.Value, true)
			}
		}
	}
	if len(fields) == 0 {
		return "", errors.New("No columns contains a value")
	}
	query += fields + " where "
	where, err := primaryKeyWhereSQL(cols, conn)
	if err != nil {
		return "", err
	}
	query += where
	return query, nil
}

//DeleteSQL builds SQL query for deleting data
func (conn *Conn) DeleteSQL(schemaName, tableName string, cols []db.Column) (string, error) {
	query := "delete from " + conn.Quote(schemaName+"."+tableName) + " where "
	where, err := primaryKeyWhereSQL(cols, conn)
	if err != nil {
		return "", err
	}
	query += where
	return query, nil
}

//primaryKeyWhereSQL returns where part (without 'where') of query
func primaryKeyWhereSQL(cols []db.Column, conn *Conn) (string, error) {
	var ret string
	for _, c := range cols {
		if c.PrimaryKey == true {
			if c.Value == nil {
				return "", errors.New("Primary key column " + c.Name + " has no value")
			}
			if len(ret) > 0 {
				ret += " and"
			}
			ret += " " + conn.Quote(c.Name) + " = " + db.Interface2string(c.Value, true)
		}
	}
	if len(ret) == 0 {
		return "", errors.New("Primary key not found in []Column")
	}
	return ret, nil
}
