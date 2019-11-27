package postgresql

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jmu0/dbAPI/db"

	//connect to postgres
	_ "github.com/lib/pq"
)

//Conn mysql connection implements db.Conn
type Conn struct {
	conn *sql.DB
}

var schemaCache map[string][]db.Column

//GetConnection returns connection *sql.DB
func (c *Conn) GetConnection() *sql.DB {
	return c.conn
}

//Connect to database
func (c *Conn) Connect(args map[string]string) error {
	if _, ok := args["hostname"]; !ok {
		return errors.New("No hostname in args")
	}
	if _, ok := args["database"]; !ok {
		return errors.New("No database in args")
	}
	if _, ok := args["username"]; !ok {
		return errors.New("No username in args")
	}
	if _, ok := args["password"]; !ok {
		return errors.New("No password in args")
	}
	if _, ok := args["port"]; !ok {
		args["port"] = "5432"
	}
	dbinfo := fmt.Sprintf("user=%s password=%s dbname=%s host=%s sslmode=disable",
		args["username"], args["password"], args["database"], args["hostname"])
	db, err := sql.Open("postgres", dbinfo)
	if err != nil {
		return err
	}
	err = db.Ping()
	if err != nil {
		return err
	}
	c.conn = db
	return nil
}

//GetSchemaNames from database
func (c *Conn) GetSchemaNames() ([]string, error) {
	dbs := []string{}
	query := "select schema_name from information_schema.schemata"
	rows, err := c.conn.Query(query)
	defer rows.Close()
	if err != nil {
		return dbs, err
	}
	if rows == nil {
		return dbs, errors.New("No databases found")
	}
	dbName := ""
	for rows.Next() {
		rows.Scan(&dbName)
		if !skipDb(dbName) {
			dbs = append(dbs, dbName)
		}
	}
	return dbs, nil
}

//don't show system databases
func skipDb(name string) bool {
	skip := []string{
		"pg_toast",
		"pg_temp_1",
		"pg_toast_temp_1",
		"pg_catalog",
		"information_schema",
	}
	for _, s := range skip {
		if name == s {
			return true
		}
	}
	return false
}

//GetTableNames from database
func (c *Conn) GetTableNames(schemaName string) ([]string, error) {
	tbls := []string{}
	query := "select table_name from information_schema.tables where table_schema='" + strings.ToLower(schemaName) + "'"
	rows, err := c.conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if rows == nil {
		return nil, errors.New("No tables found in " + schemaName)
	}
	tableName := ""
	for rows.Next() {
		rows.Scan(&tableName)
		tbls = append(tbls, tableName)
	}
	return tbls, nil
}

//GetRelationships from database table
func (c *Conn) GetRelationships(schemaName string, tableName string) ([]db.Relationship, error) {
	var ret []db.Relationship
	query := fmt.Sprintf(`select fromTbl, string_agg(distinct(fromCol),', ') as fromCols, toTbl, string_agg(distinct(toCol), ', ') as toCols from (SELECT
		concat(tc.table_schema, '.', tc.table_name) as fromTbl, 
		kcu.column_name as fromCol, 
		concat(ccu.table_schema, '.', ccu.table_name) AS toTbl,
		ccu.column_name AS toCol
	FROM 
		information_schema.table_constraints AS tc 
		JOIN information_schema.key_column_usage AS kcu
		  ON tc.constraint_name = kcu.constraint_name
		  AND tc.table_schema = kcu.table_schema
		JOIN information_schema.constraint_column_usage AS ccu
		  ON ccu.constraint_name = tc.constraint_name
		  AND ccu.table_schema = tc.table_schema
	WHERE tc.constraint_type = 'FOREIGN KEY' 
	AND ((tc.table_schema='%s' and tc.table_name='%s') 
		or (ccu.table_schema='%s' and ccu.table_name='%s'))) as regels
		group by fromTbl, toTbl`, schemaName, tableName, schemaName, tableName)
	res, err := db.Query(c, query)
	//TODO: check if this matters: on constraints with multiple columns the order of the columns can be different
	if err != nil {
		return ret, err
	}
	for _, r := range res {
		var rel = db.Relationship{
			FromTable: r["fromtbl"].(string),
			FromCols:  r["fromcols"].(string),
			ToTable:   r["totbl"].(string),
			ToCols:    r["tocols"].(string),
		}
		if rel.FromTable == schemaName+"."+tableName {
			rel.Cardinality = "many-to-one"
		} else if rel.ToTable == schemaName+"."+tableName {
			rel.Cardinality = "one-to-many"
		}
		ret = append(ret, rel)
	}
	return ret, nil
}

//GetColumns from database table
func (c *Conn) GetColumns(schemaName, tableName string) ([]db.Column, error) {
	if schemaCache == nil {
		schemaCache = make(map[string][]db.Column)
	}
	if _, ok := schemaCache[schemaName+"."+tableName]; ok {
		return schemaCache[schemaName+"."+tableName], nil
	}
	query := fmt.Sprintf(`select c.column_name,
	c.data_type, c.character_maximum_length, c.is_nullable, c.column_default,
	COALESCE((select tc.constraint_type from information_schema.key_column_usage kc
	inner join information_schema.table_constraints tc 
		on kc.constraint_catalog=tc.constraint_catalog and kc.constraint_schema=tc.constraint_schema and kc.constraint_name=tc.constraint_name
	where tc.table_catalog=c.table_catalog and tc.table_schema=c.table_schema and  tc.table_name=c.table_name and kc.column_name = c.column_name
	and tc.constraint_type='PRIMARY KEY'),'') as key
	from information_schema.columns c
	where c.table_schema='%s' and c.table_name='%s'`, schemaName, tableName)
	cols := []db.Column{}
	var col db.Column
	var name, tp, ln, null, def, key interface{}
	var l int
	var err error
	rows, err := c.conn.Query(query)
	defer rows.Close()
	if err == nil && rows != nil {
		for rows.Next() {
			name = ""
			tp = ""
			ln = ""
			null = ""
			def = nil
			key = nil
			rows.Scan(&name, &tp, &ln, &null, &def, &key)
			l, err = strconv.Atoi(db.Interface2string(ln, false))
			if err != nil {
				l = 0
			}

			if def == nil {
				def = ""
			}
			col = db.Column{
				Name:         db.Interface2string(name, false),
				DefaultValue: db.Interface2string(def, false),
				Length:       l,
			}
			if key == "PRIMARY KEY" {
				col.PrimaryKey = true
			} else {
				col.PrimaryKey = false
			}
			if null == "YES" {
				col.Nullable = true
			} else {
				col.Nullable = false
			}
			if strings.Contains(def.(string), "nextval") {
				col.AutoIncrement = true
			} else {
				col.AutoIncrement = false
			}
			col.Type = mapDataType(db.Interface2string(tp, false))
			if col.Type == "dbdate" {
				col.Length = 10
			}
			cols = append(cols, col)
		}
	}
	if len(cols) == 0 {
		return cols, errors.New("No columns found")
	}
	schemaCache[schemaName+"."+tableName] = cols
	return cols, nil
}

func mapDataType(dbType string) string {
	//TODO: date datatype
	dataTypes := map[string]string{
		"smallint":          "int",
		"integer":           "int",
		"bigint":            "int",
		"decimal":           "float",
		"numeric":           "float",
		"real":              "float",
		"double precision":  "float",
		"smallserial":       "int",
		"serial":            "int",
		"bigserial":         "int",
		"money":             "float",
		"character varying": "string",
		"varchar":           "string",
		"character":         "string",
		"char":              "string",
		"text":              "string",
		"timestamp":         "string",
		"date":              "dbdate",
		"time":              "string",
		"boolean":           "bool",
	}
	if t, ok := dataTypes[dbType]; ok {
		return t
	}
	return "unknown"
}
