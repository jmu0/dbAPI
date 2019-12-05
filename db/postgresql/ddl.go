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
			primaryKey += c.Quote(col.Name)
		}
	}
	if len(primaryKey) > 0 {
		query += ",\n\tprimary key (" + primaryKey + ")"
	}
	for i, r := range tbl.ForeignKeys {
		db.SetForeignKeyName(&r)
		tbl.ForeignKeys[i] = r
		query += ",\n\tconstraint " + c.Quote(r.Name)
		query += " foreign key (" + c.Quote(r.FromCols) + ") references " + c.Quote(r.ToTable) + " (" + c.Quote(r.ToCols) + ") deferrable"
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
	db.SetIndexName(schemaName, tableName, index)
	query := "create index " + c.Quote(index.Name) + " on " + c.Quote(schemaName+"."+tableName)
	query += " (" + c.Quote(index.Columns) + ");"
	return query
}

//DropIndexSQL drop index sql
func (c *Conn) DropIndexSQL(schemaName, tableName, indexName string) string {
	return "drop index \"" + schemaName + "\".\"" + indexName + "\";"
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
	var query string
	alter := "alter table " + c.Quote(schemaName+"."+tableName)
	orig, err := getColumn(schemaName, tableName, col.Name, c)
	if err != nil {
		return "", err
	}

	if col.AutoIncrement == true && orig.AutoIncrement == false {
		return "", errors.New("Cannot change col " + orig.Name + " to auto-increment.")
	}
	if col.AutoIncrement == false && orig.AutoIncrement == true {
		query += alter + "\n\t alter column " + c.Quote(col.Name) + " drop default;"
		return query, nil
	}
	if col.AutoIncrement == true && orig.AutoIncrement == true {
		return "", nil
	}

	if orig.Type != col.Type || orig.Length != col.Length {
		query += alter + "\n\t"
		query += "alter column " + c.Quote(col.Name) + " type"
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
		if (orig.Type == "dbdate" && col.DefaultValue == "CURRENT_TIMESTAMP") == false { //skip current timestamp cols
			if len(query) > 0 {
				query += "\n"
			}
			query += alter + "\n\talter column " + c.Quote(col.Name)
			if col.DefaultValue == "" {
				query += " drop default"
			} else {
				query += " set default '" + col.DefaultValue + "'"
			}
			query += ";"
		}
	}
	if orig.Nullable != col.Nullable {
		if len(query) > 0 {
			query += "\n"
		}
		query += alter + "\n\talter column " + c.Quote(col.Name)
		if col.Nullable == true {
			query += " drop not null"
		} else {
			query += " set not null"
		}
		query += ";"
	}
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
	query += " foreign key (" + c.Quote(fk.FromCols) + ") references " + c.Quote(fk.ToTable)
	query += " (" + c.Quote(fk.ToCols) + ");"
	return query
}

//DropForeignKeySQL returns sql to drop foreign key from table
func (c *Conn) DropForeignKeySQL(schemaName, tableName, keyName string) string {
	query := "alter table " + c.Quote(schemaName+"."+tableName) + "\n\t"
	query += "drop constraint " + c.Quote(keyName) + ";"
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
			ret += " date"
			if strings.Contains(col.DefaultValue, "CURRENT_TIMESTAMP") {
				col.DefaultValue = "NOW()"
			}
		default:
			return "", errors.New("invalid type" + col.Type)
		}
	} else {
		ret += " serial"
	}
	if col.Nullable == false {
		ret += " not null"
	}
	if len(col.DefaultValue) > 0 {
		ret += " default '" + col.DefaultValue + "'"
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
