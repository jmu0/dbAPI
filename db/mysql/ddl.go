package mysql

import (
	"errors"
	"strconv"
	"strings"

	"github.com/jmu0/dbAPI/db"
)

//PreSQL sql to put at start of query
func (c *Conn) PreSQL() string {
	return "set foreign_key_checks=0;"
}

//PostSQL sql to put at end of query
func (c *Conn) PostSQL() string {
	return "set foreign_key_checks=1;"
}

//CreateSchemaSQL get create schema sql
func (c *Conn) CreateSchemaSQL(schemaName string) string {
	return "create database if not exists " + schemaName + ";"
}

//DropSchemaSQL get drop schema sql
func (c *Conn) DropSchemaSQL(schemaName string) string {
	return "drop database if exists " + schemaName + ";"
}

// CreateTableSQL get create table SQL
func (c *Conn) CreateTableSQL(tbl *db.Table) (string, error) {
	var query string
	var primaryKey string
	query = "create table " + tbl.Schema + "." + tbl.Name + " ("
	for i, c := range tbl.Columns {
		csql, err := columnSQL(&c)
		if err != nil {
			return "", err
		}
		if i > 0 {
			query += ","
		}
		query += "\n\t" + csql
		if c.PrimaryKey == true {
			if len(primaryKey) > 0 {
				primaryKey += ","
			}
			primaryKey += c.Name
		}
	}
	if len(primaryKey) > 0 {
		query += ",\n\tprimary key (" + primaryKey + ")"
	}
	for _, r := range tbl.ForeignKeys {
		query += ",\n\tconstraint " + strings.Replace(tbl.Name, ".", "_", -1) + "_" + strings.Replace(strings.Replace(r.FromCols, ", ", "_", -1), ",", "_", -1) + "_fkey"
		query += " foreign key (" + r.FromCols + ") references " + r.ToTable + " (" + r.ToCols + ")"
	}
	query += "\n);"
	for i, ind := range tbl.Indexes {
		if i == 0 {
			query += "\n"
		}
		query += c.CreateIndexSQL(tbl.Schema, tbl.Name, &ind)
		if i < len(tbl.Indexes) {
			query += "\n"
		}
	}
	return query, nil
}

//DropTableSQL get drop table SQL
func (c *Conn) DropTableSQL(tbl *db.Table) string {
	return "drop table if exists " + tbl.Schema + "." + tbl.Name + ";"
}

//CreateIndexSQL get create index sql
func (c *Conn) CreateIndexSQL(schemaName, tableName string, index *db.Index) string {
	db.SetIndexName(schemaName, tableName, index)
	query := "create index " + index.Name + " on " + schemaName + "." + tableName
	query += " (" + index.Columns + ");"
	return query
}

//DropIndexSQL drop index sql
func (c *Conn) DropIndexSQL(schemaName, tableName, indexName string) string {
	return "drop index " + indexName + " on " + schemaName + "." + tableName + ";"
}

//AddColumnSQL returns sql to add a column
func (c *Conn) AddColumnSQL(schemaName, tableName string, col *db.Column) (string, error) {
	query := "alter table " + schemaName + "." + tableName
	tmp, err := columnSQL(col)
	if err != nil {
		return "", err
	}
	query += "\n\tadd " + tmp + ";"
	return query, nil
}

//DropColumnSQL returns sql to drop a column
func (c *Conn) DropColumnSQL(schemaName, tableName, columnName string) string {
	query := "alter table " + schemaName + "." + tableName
	query += "\n\tdrop column " + columnName + ";"
	return query
}

//AlterColumnSQL returns sql to alter column
func (c *Conn) AlterColumnSQL(schemaName, tableName string, col *db.Column) (string, error) {
	query := "alter table " + schemaName + "." + tableName
	tmp, err := columnSQL(col)
	if err != nil {
		return "", err
	}
	query += "\n\tmodify " + tmp + ";"
	return query, nil
}

//AddForeignKeySQL returns sql to add foreign key to table
func (c *Conn) AddForeignKeySQL(schemaName, tableName string, fk *db.ForeignKey) string {
	if fk.Name == "" {
		fk.Name = strings.Replace(tableName, ".", "_", -1) + "_"
		fk.Name += strings.Replace(strings.Replace(fk.FromCols, ", ", "_", -1), ",", "_", -1) + "_fkey"
	}
	query := "alter table " + schemaName + "." + tableName + "\n\t"
	query += "add constraint " + fk.Name
	query += " foreign key (" + fk.FromCols + ") references " + fk.ToTable + " (" + fk.ToCols + ");"
	return query
}

//DropForeignKeySQL returns sql to drop foreign key from table
func (c *Conn) DropForeignKeySQL(schemaName, tableName, keyName string) string {
	query := "alter table " + schemaName + "." + tableName + "\n\t"
	query += "drop foreign key " + keyName + ";"
	return query
}

func columnSQL(c *db.Column) (string, error) {
	var ret = c.Name
	if c.AutoIncrement == false {
		switch c.Type {
		case "string":
			ret += " varchar(" + strconv.Itoa(c.Length) + ")"
		case "int":
			ret += " int"
		case "float":
			ret += " numeric"
		case "bool":
			ret += " boolean"
		case "dbdate":
			ret += " datetime"
		default:
			return "", errors.New("invalid type " + c.Type)
		}
	} else {
		ret += " serial"
	}

	if c.Nullable == false {
		ret += " not null"
	}

	if len(c.DefaultValue) > 0 {

		ret += " default '" + c.DefaultValue + "'"
	}
	return ret, nil
}
