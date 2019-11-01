package db

import (
	"errors"
)

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
