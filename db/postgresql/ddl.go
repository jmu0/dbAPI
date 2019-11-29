package postgresql

import (
	"errors"
	"strconv"
	"strings"

	"github.com/jmu0/dbAPI/db"
)

//PreSQL sql to put at start of query
func (c *Conn) PreSQL() string {
	return ""
}

//PostSQL sql to put at end of query
func (c *Conn) PostSQL() string {
	return ""
}

//CreateSchemaSQL get create schema sql
func (c *Conn) CreateSchemaSQL(schemaName string) string {
	return "create schema if not exists \"" + schemaName + "\";"
}

//DropSchemaSQL get drop schema sql
func (c *Conn) DropSchemaSQL(schemaName string) string {
	return "drop schema if exists \"" + schemaName + "\" cascade;"
}

// CreateTableSQL get create table SQL
func (c *Conn) CreateTableSQL(tbl *db.Table) (string, error) {
	var query string
	var primaryKey string
	query = "create table " + db.DoubleQuote(tbl.Schema+"."+tbl.Name) + " ("
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
			primaryKey += db.DoubleQuote(c.Name)
		}
	}
	if len(primaryKey) > 0 {
		query += ",\n\tprimary key (" + primaryKey + ")"
	}
	for _, r := range tbl.ForeignKeys {
		query += ",\n\tconstraint " + strings.Replace(tbl.Name, ".", "_", -1) + "_" + strings.Replace(strings.Replace(r.FromCols, ", ", "_", -1), ",", "_", -1) + "_fkey"
		query += " foreign key (" + db.DoubleQuote(r.FromCols) + ") references " + db.DoubleQuote(r.ToTable) + " (" + db.DoubleQuote(r.ToCols) + ") deferrable"
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
	return "drop table if exists \"" + tbl.Schema + "\".\"" + tbl.Name + "\";"
}

//CreateIndexSQL get create index sql
func (c *Conn) CreateIndexSQL(schemaName, tableName string, index *db.Index) string {
	if len(index.Name) == 0 || index.Name == index.Columns {
		index.Name = schemaName + "_" + tableName + "_" + strings.Replace(index.Columns, ", ", "_", -1) + "_index"
	}
	query := "create index " + db.DoubleQuote(index.Name) + " on " + db.DoubleQuote(schemaName+"."+tableName)
	query += " (" + db.DoubleQuote(index.Columns) + ");"
	return query
}

//DropIndexSQL drop index sql
func (c *Conn) DropIndexSQL(schemaName, tableName, indexName string) string {
	return "drop index \"" + schemaName + "\".\"" + indexName + "\";"
}

//AddColumnSQL returns sql to add a column
func (c *Conn) AddColumnSQL(schemaName, tableName string, col *db.Column) (string, error) {
	query := "alter table " + db.DoubleQuote(schemaName+"."+tableName)
	tmp, err := columnSQL(col)
	if err != nil {
		return "", err
	}
	query += "\n\tadd " + tmp
	return query, nil
}

//DropColumnSQL returns sql to drop a column
func (c *Conn) DropColumnSQL(schemaName, tableName, columnName string) string {
	query := "alter table " + db.DoubleQuote(schemaName+"."+tableName)
	query += "\n\tdrop column " + db.DoubleQuote(columnName) + ";"
	return query
}

//AlterColumnSQL returns sql to alter column
func (c *Conn) AlterColumnSQL(schemaName, tableName string, col *db.Column) (string, error) {
	var query string
	alter := "alter table " + db.DoubleQuote(schemaName+"."+tableName)
	orig, err := getColumn(schemaName, tableName, col.Name, c)
	if err != nil {
		return "", err
	}
	if orig.Type != col.Type || orig.Length != col.Length {
		query += alter + "\n\t"
		query += "alter column " + db.DoubleQuote(col.Name) + " type"
		switch col.Type {
		case "string":
			query += " varchar(" + strconv.Itoa(col.Length) + ")"
		case "int":
			query += " int"
		case "float":
			query += " numeric"
		case "bool":
			query += " boolean"
		case "dbdate":
			query += " date"
			if strings.Contains(col.DefaultValue, "CURRENT_TIMESTAMP") {
				col.DefaultValue = "NOW()"
			}
		default:
			return "", errors.New("invalid type" + col.Type)
		}
		query += ";"
	}
	if orig.DefaultValue != col.DefaultValue {
		if len(query) > 0 {
			query += "\n"
		}
		query += alter + "\n\talter column " + db.DoubleQuote(col.Name)
		if col.DefaultValue == "" {
			query += " drop default"
		} else {
			query += " set default '" + col.DefaultValue + "'"
		}
		query += ";"
	}
	if orig.Nullable != col.Nullable {
		if len(query) > 0 {
			query += "\n"
		}
		query += alter + "\n\talter column " + db.DoubleQuote(col.Name)
		if col.Nullable == true {
			query += " drop not null"
		} else {
			query += " set not null"
		}
		query += ";"
	}
	return query, nil
}

//AddForeignKeySQL returns sql to add a foreign key

//DropForeignKeySQL returns sql to drop a foreign key

func columnSQL(c *db.Column) (string, error) {
	var ret = "\"" + c.Name + "\""
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
			ret += " date"
			if strings.Contains(c.DefaultValue, "CURRENT_TIMESTAMP") {
				c.DefaultValue = "NOW()"
			}
		default:
			return "", errors.New("invalid type" + c.Type)
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

func getColumn(schemaName, tableName, columnName string, conn *Conn) (db.Column, error) {
	cols, err := conn.GetColumns(schemaName, tableName)
	if err != nil {
		return db.Column{}, err
	}
	for _, c := range cols {
		if c.Name == columnName {
			return c, nil
		}
	}
	return db.Column{}, errors.New("Column " + columnName + " not found")
}
