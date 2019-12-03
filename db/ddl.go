package db

import (
	"errors"
	"log"
)

//GetTable reads table struct from database
func GetTable(schemaName, tableName string, conn Conn) (Table, error) {
	var err error
	var rels []Relationship
	var fk ForeignKey
	var tbl = Table{
		Name:   tableName,
		Schema: schemaName,
	}
	tbl.Columns, err = conn.GetColumns(schemaName, tableName)
	if err != nil {
		return Table{}, err
	}
	rels, err = conn.GetRelationships(schemaName, tableName)
	for _, r := range rels {
		if r.Cardinality == "many-to-one" {
			fk = ForeignKey{
				Name:     r.Name,
				FromCols: r.FromCols,
				ToTable:  r.ToTable,
				ToCols:   r.ToCols,
			}
			tbl.ForeignKeys = append(tbl.ForeignKeys, fk)
		}
	}
	if err != nil {
		return Table{}, err
	}
	tbl.Indexes, err = conn.GetIndexes(schemaName, tableName)
	if err != nil {
		return Table{}, err
	}
	return tbl, nil
}

//GetSchema reads schema from database
func GetSchema(schemaName string, conn Conn) (Schema, error) {
	var tbls []string
	var err error
	var tbl Table

	var s = Schema{
		Name: schemaName,
	}
	tbls, err = conn.GetTableNames(schemaName)
	if err != nil {
		return Schema{}, err
	}
	for _, t := range tbls {
		tbl, err = GetTable(schemaName, t, conn)
		if err != nil {
			return Schema{}, err
		}
		s.Tables = append(s.Tables, tbl)
	}
	return s, nil
}

//UpdateTableSQL compares table struct to database and returns SQL to modify/create table in database
func UpdateTableSQL(tbl *Table, conn Conn, updateSchema bool) (string, error) {
	var sql, tmp string
	var toTable Table
	var err error
	if updateSchema && HasSchema(tbl.Schema, conn) == false {
		tmp := conn.CreateSchemaSQL(tbl.Schema)
		if len(sql) > 0 {
			sql += "\n"
		}
		sql += tmp
	}
	if HasTable(tbl.Schema, tbl.Name, conn) == false {
		tmp, err = conn.CreateTableSQL(tbl)
		if err != nil {
			return "", err
		}
		if len(sql) > 0 {
			sql += "\n"
		}
		sql += tmp
	} else {
		toTable, err = GetTable(tbl.Schema, tbl.Name, conn)
		if err != nil {
			return "", err
		}
		tmp, err = compareCols(tbl.Schema, tbl.Name, tbl.Columns, toTable.Columns, conn)
		if err != nil {
			return "", err
		}
		if len(sql) > 0 {
			sql += "\n"
		}
		sql += tmp
		tmp = compareIndexes(tbl.Schema, tbl.Name, tbl.Indexes, toTable.Indexes, conn)
		if len(tmp) > 0 {
			if len(sql) > 0 {
				sql += "\n"
			}
			sql += tmp
		}
		//TODO test compare indexes
		//TODO compare foreign keys
	}

	return sql, nil
}

//UpdateSchemaSQL compares schema struct to database and returns SQL to modify/create schema in database
func UpdateSchemaSQL(schema *Schema, conn Conn) (string, error) {
	var sql, tmp string
	var err error
	if HasSchema(schema.Name, conn) == false {
		log.Fatal("create schema:", schema.Name)
		tmp := conn.CreateSchemaSQL(schema.Name)
		if len(sql) > 0 {
			sql += "\n"
		}
		sql += tmp
	}
	SortTablesByForeignKey(schema.Tables)
	for _, tbl := range schema.Tables {
		tmp, err = UpdateTableSQL(&tbl, conn, false)
		if err != nil {
			return "", err
		}
		if len(tmp) > 0 {
			sql += "\n" + tmp
		}
	}
	return sql, nil
}

//compares columns and returns sql to add/remove/update a column in the database
func compareCols(schemaName, tableName string, from, to []Column, conn Conn) (string, error) {
	var ret, tmp string
	var err error
	var toCol Column
	for _, c := range from {
		if toCol, err = findCol(c.Name, to); err == nil {
			if toCol.Type != c.Type ||
				(toCol.Length != c.Length && c.Type == "string") ||
				toCol.DefaultValue != c.DefaultValue ||
				toCol.Nullable != c.Nullable {
				tmp, err = conn.AlterColumnSQL(schemaName, tableName, &c)
				if err != nil {
					return "", err
				}
				if len(ret) > 0 {
					ret += "\n"
				}
				ret += tmp

			}
		} else {
			tmp, err = conn.AddColumnSQL(schemaName, tableName, &c)
			if err != nil {
				return "", err
			}
			if len(ret) > 0 {
				ret += "\n"
			}
			ret += tmp
		}
	}
	for _, c := range to {
		if toCol, err = findCol(c.Name, from); err != nil {
			if len(ret) > 0 {
				ret += "\n"
			}
			ret += conn.DropColumnSQL(schemaName, tableName, c.Name)
		}
	}
	return ret, nil
}

//compares indexes and returns sql to add/remove/update an index in the database
func compareIndexes(schemaName, tableName string, from, to []Index, conn Conn) string {
	var ret, tmp string
	var err error
	var toIndex Index
	for j, i := range from {
		SetIndexName(schemaName, tableName, &i)
		from[j] = i
		if toIndex, err = findIndex(i.Name, to); err == nil {
			if toIndex.Columns != i.Columns {
				tmp = conn.DropIndexSQL(schemaName, tableName, i.Name)
				tmp += "\n" + conn.CreateIndexSQL(schemaName, tableName, &i)
				if len(ret) > 0 {
					ret += "\n"
				}
				ret += tmp
			}
		} else {
			tmp = conn.CreateIndexSQL(schemaName, tableName, &i)
			if len(ret) > 0 {
				ret += "\n"
			}
			ret += tmp
		}
	}
	for _, i := range to {
		if toIndex, err = findIndex(i.Name, from); err != nil {
			if len(ret) > 0 {
				ret += "\n"
			}
			ret += conn.DropIndexSQL(schemaName, tableName, i.Name)
		}
	}
	return ret
}

func findCol(colName string, cols []Column) (Column, error) {
	for _, c := range cols {
		if c.Name == colName {
			return c, nil
		}
	}
	return Column{}, errors.New("Column " + colName + " not found in given columns")
}
func findIndex(iName string, indexes []Index) (Index, error) {
	for _, i := range indexes {
		if i.Name == iName {
			return i, nil
		}
	}
	return Index{}, errors.New("Index " + iName + " not found in given indexes")
}
