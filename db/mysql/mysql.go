package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/jmu0/dbAPI/db"
	//connect to mysql
	_ "github.com/go-sql-driver/mysql"
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
	if _, ok := args["username"]; !ok {
		return errors.New("No username in args")
	}
	if _, ok := args["password"]; !ok {
		return errors.New("No password in args")
	}
	if _, ok := args["port"]; !ok {
		args["port"] = "3306"
	}
	dsn := args["username"] + ":" + args["password"] + "@tcp(" + args["hostname"] + ":" + args["port"] + ")/"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	err = db.Ping()
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(0)
	d, _ := time.ParseDuration("1 second")
	db.SetConnMaxLifetime(d)
	c.conn = db
	return nil
}

//Execute executes query without returning results. returns (lastInsertId, rowsAffected, error)
func (c *Conn) Execute(query string) (int64, int64, error) {
	// fmt.Println(query)
	res, err := c.GetConnection().Exec(query)
	if err != nil {
		return 0, 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, 0, err
	}

	return id, n, nil
}

//Query queries the database
func (c *Conn) Query(query string) ([]map[string]interface{}, error) {
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

//GetSchemaNames from database
func (c *Conn) GetSchemaNames() ([]string, error) {
	dbs := []string{}
	query := "show databases"
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
		"information_schema",
		"mysql",
		"performance_schema",
		"owncloud",
		"nextcloud",
		"roundcubemail",
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
	query := "show full tables in " + schemaName + " where Table_type='BASE TABLE'"
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
		rows.Scan(&tableName, nil)
		tbls = append(tbls, tableName)
	}
	return tbls, nil
}

//GetRelationships from database table
func (c *Conn) GetRelationships(schemaName string, tableName string) ([]db.Relationship, error) {
	var ret []db.Relationship
	var rel db.Relationship
	var query = `select constraint_name as name, concat(table_schema, ".", table_name) as fromTbl, 
			group_concat(column_name separator ", ") as fromCols,
			concat(referenced_table_schema, ".", referenced_table_name) as toTbl, 
			group_concat(referenced_column_name separator ", ") as toCols
			from (select constraint_name, table_schema,table_name,column_name,referenced_table_schema,referenced_table_name, 
			referenced_column_name from information_schema.key_column_usage
			where (referenced_table_schema="` + schemaName + `" and referenced_table_name="` + tableName + `") 
			or (table_schema="` + schemaName + `" and table_name="` + tableName + `")
			and constraint_name <> "PRIMARY"
			) as relations group by constraint_name, fromTbl, toTbl`
	// log.Fatal(query)
	res, err := c.Query(query)
	if err != nil {
		return ret, err
	}
	for _, r := range res {
		rel = db.Relationship{
			Name:      r["name"].(string),
			FromTable: r["fromTbl"].(string),
			FromCols:  r["fromCols"].(string),
			ToTable:   r["toTbl"].(string),
			ToCols:    r["toCols"].(string),
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

//GetIndexes get indexes for table
func (c *Conn) GetIndexes(schemaName, tableName string) ([]db.Index, error) {
	var ret = []db.Index{}
	var ind db.Index
	query := fmt.Sprintf(`
	select 
		index_name as 'index',
		group_concat(column_name order by seq_in_index separator ', ') as columns
	from information_schema.statistics
	group by 
		table_schema,
		table_name,
		index_name
	having
		table_schema = '%s'
		and table_name = '%s'
		and index_name <> 'PRIMARY'
	`, schemaName, tableName)
	// log.Fatal(query)
	res, err := c.Query(query)
	if err != nil {
		return ret, err
	}
	for _, r := range res {
		ind = db.Index{
			Name:    r["index"].(string),
			Columns: r["columns"].(string),
		}
		ret = append(ret, ind)
	}
	return ret, nil
}

//GetColumns from database table
func (c *Conn) GetColumns(schemaName, tableName string) ([]db.Column, error) {
	if schemaCache == nil {
		schemaCache = make(map[string][]db.Column)
	}
	if _, ok := schemaCache[schemaName+"."+tableName]; ok {
		for i := range schemaCache[schemaName+"."+tableName] {
			schemaCache[schemaName+"."+tableName][i].Value = ""
		}
		return schemaCache[schemaName+"."+tableName], nil
	}
	cols := []db.Column{}
	var col db.Column
	var field, tp, null, key, def, extra interface{}
	query := "show columns from " + schemaName + "." + tableName
	rows, err := c.conn.Query(query)
	if err != nil {
		return cols, err
	}
	defer rows.Close()
	if err == nil && rows != nil {
		for rows.Next() {
			rows.Scan(&field, &tp, &null, &key, &def, &extra)
			col = db.Column{
				Name:         db.Interface2string(field, false),
				DefaultValue: db.Interface2string(def, false),
			}
			if db.Interface2string(key, false) == "PRI" {
				col.PrimaryKey = true
			} else {
				col.PrimaryKey = false
			}
			if db.Interface2string(null, false) == "YES" {
				col.Nullable = true
			} else {
				col.Nullable = false
			}
			if db.Interface2string(extra, false) == "auto_increment" {
				col.AutoIncrement = true
			} else {
				col.AutoIncrement = false
			}
			col.Type, col.Length = mapDataType(db.Interface2string(tp, false))
			cols = append(cols, col)
		}
	}

	schemaCache[schemaName+"."+tableName] = cols
	return cols, nil
}

func mapDataType(dbType string) (string, int) {
	var spl = strings.Split(dbType, "(")
	var tp string
	var ln int
	var err error
	if len(spl) > 1 {
		tp = spl[0]
		ln, err = strconv.Atoi(spl[1][:len(spl[1])-1])
		if err != nil {
			ln = 0
		}
	} else {
		tp = dbType
	}
	dataTypes := map[string]string{
		"varchar":   "string",
		"text":      "string",
		"longtext":  "string",
		"char":      "string",
		"int":       "int",
		"tinyint":   "int",
		"smallint":  "int",
		"bigint":    "int",
		"date":      "dbdate",
		"datetime":  "dbdate",
		"timestamp": "dbdate",
		"float":     "float",
		"double":    "float",
		"decimal":   "float",
	}
	if t, ok := dataTypes[tp]; ok {
		if t == "dbdate" {
			ln = 10
		}
		if tp == "text" {
			ln = 10000
		}
		if tp == "longtext" {
			ln = 100000
		}

		return t, ln
	}
	log.Println("WARNING: unmapped datatype: ", tp)
	return "unknown:" + tp, ln
}

//Quote puts quotes around string for in SQL
func (c *Conn) Quote(str string) string {
	var res, sep string
	var spl []string
	if strings.Contains(str, ",") {
		sep = ", "
		spl = strings.Split(str, ",")
	} else if strings.Contains(str, ".") {
		sep = "."
		spl = strings.Split(str, ".")
	} else {
		return "`" + str + "`"
	}
	for _, item := range spl {
		if len(res) > 0 {
			res += sep
		}
		res += "`" + strings.TrimSpace(item) + "`"
	}
	return res
}
