package mysql

import (
	"errors"
	"strconv"

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
	return "create database if not exists " + c.Quote(schemaName) + ";"
}

//DropSchemaSQL get drop schema sql
func (c *Conn) DropSchemaSQL(schemaName string) string {
	return "drop database if exists " + c.Quote(schemaName) + ";"
}

// CreateTableSQL get create table SQL
func (c *Conn) CreateTableSQL(tbl *db.Table) (string, error) {
	var query string
	var primaryKey string
	query = "create table " + c.Quote(tbl.Schema+"."+tbl.Name) + " ("
	for i, col := range tbl.Columns {
		csql, err := c.columnSQL(&col)
		if err != nil {
			return "", err
		}
		if i > 0 {
			query += ","
		}
		query += "\n\t" + csql
		if col.PrimaryKey == true {
			if len(primaryKey) > 0 {
				primaryKey += ","
			}
			primaryKey += col.Name
		}
	}
	if len(primaryKey) > 0 {
		query += ",\n\tprimary key (" + c.Quote(primaryKey) + ")"
	}
	for i, r := range tbl.ForeignKeys {
		db.SetForeignKeyName(&r)
		tbl.ForeignKeys[i] = r
		query += ",\n\tconstraint " + c.Quote(r.Name)
		query += " foreign key (" + c.Quote(r.FromCols) + ") references " + r.ToTable + " (" + r.ToCols + ")"
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
	return "drop table if exists " + c.Quote(tbl.Schema+"."+tbl.Name) + ";"
}

//CreateIndexSQL get create index sql
func (c *Conn) CreateIndexSQL(schemaName, tableName string, index *db.Index) string {
	db.SetIndexName(schemaName, tableName, index)
	query := "create index " + c.Quote(index.Name) + " on " + c.Quote(schemaName+"."+tableName)
	query += " (" + c.Quote(index.Columns) + ");"
	return query
}

//DropIndexSQL drop index sql
func (c *Conn) DropIndexSQL(schemaName, tableName, indexName string) string {
	return "drop index " + c.Quote(indexName) + " on " + c.Quote(schemaName+"."+tableName) + ";"
}

//AddColumnSQL returns sql to add a column
func (c *Conn) AddColumnSQL(schemaName, tableName string, col *db.Column) (string, error) {
	query := "alter table " + c.Quote(schemaName+"."+tableName)
	tmp, err := c.columnSQL(col)
	if err != nil {
		return "", err
	}
	query += "\n\tadd " + tmp + ";"
	return query, nil
}

//DropColumnSQL returns sql to drop a column
func (c *Conn) DropColumnSQL(schemaName, tableName, columnName string) string {
	query := "alter table " + c.Quote(schemaName+"."+tableName)
	query += "\n\tdrop column " + c.Quote(columnName) + ";"
	return query
}

//AlterColumnSQL returns sql to alter column
func (c *Conn) AlterColumnSQL(schemaName, tableName string, col *db.Column) (string, error) {
	query := "alter table " + c.Quote(schemaName+"."+tableName)
	tmp, err := c.columnSQL(col)
	if err != nil {
		return "", err
	}
	query += "\n\tmodify " + tmp + ";"
	return query, nil
}

//AddForeignKeySQL returns sql to add foreign key to table
func (c *Conn) AddForeignKeySQL(schemaName, tableName string, fk *db.ForeignKey) string {
	db.SetForeignKeyName(fk)
	// if fk.Name == "" {
	// 	fk.Name = strings.Replace(tableName, ".", "_", -1) + "_"
	// 	fk.Name += strings.Replace(strings.Replace(fk.FromCols, ", ", "_", -1), ",", "_", -1) + "_fkey"
	// }
	query := "alter table " + c.Quote(schemaName+"."+tableName) + "\n\t"
	query += "add constraint " + c.Quote(fk.Name)
	query += " foreign key (" + c.Quote(fk.FromCols) + ") references " + c.Quote(fk.ToTable) + " (" + c.Quote(fk.ToCols) + ");"
	return query
}

//DropForeignKeySQL returns sql to drop foreign key from table
func (c *Conn) DropForeignKeySQL(schemaName, tableName, keyName string) string {
	query := "alter table " + c.Quote(schemaName+"."+tableName) + "\n\t"
	query += "drop foreign key " + c.Quote(keyName) + ";"
	return query
}

func (c *Conn) columnSQL(col *db.Column) (string, error) {
	var ret = c.Quote(col.Name)
	if col.AutoIncrement == false {
		switch col.Type {
		case "string":
			ret += " varchar(" + strconv.Itoa(col.Length) + ")"
		case "int":
			ret += " int"
		case "float":
			ret += " numeric"
		case "bool":
			ret += " boolean"
		case "dbdate":
			ret += " datetime"
		default:
			return "", errors.New("invalid type " + col.Type)
		}
		if col.Nullable == false {
			ret += " not null"
		}
	} else {
		ret += " int not null auto_increment"
	}

	if len(col.DefaultValue) > 0 {
		if col.DefaultValue == "CURRENT_TIMESTAMP" {
			ret += " default current_timestamp"
		} else {
			ret += " default '" + col.DefaultValue + "'"
		}
	}
	return ret, nil
}
