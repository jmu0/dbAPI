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

//Execute executes query without returning results. returns (lastInsertId, rowsAffected, error)
func (c *Conn) Execute(query string) (int64, int64, error) {
	var id, n int64
	if strings.ToLower(query[:6]) == "insert" {
		//Fake lastinsertid
		rows, err := c.GetConnection().Query(query)
		if err != nil {
			return 0, 0, err
		}
		for rows.Next() {
			rows.Scan(&id)
		}
		return id, 1, nil
	}
	res, err := c.GetConnection().Exec(query)
	if err != nil {
		return 0, 0, err
	}
	n, err = res.RowsAffected()
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
	query := "select schema_name from information_schema.schemata"
	rows, err := c.conn.Query(query)
	defer rows.Close()
	if err != nil {
		return dbs, err
	}
	if rows == nil {
		return dbs, errors.New("No schemas found")
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
	query := "select table_name from information_schema.tables where table_schema='" + schemaName + "'"
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
	var rel db.Relationship
	query := fmt.Sprintf(`
	select 
  name, 
  fromTbl, 
  string_agg(fromCol,', ' order by ordinal_position) as fromCols,
  toTbl,
  string_agg(toCol,', ' order by ordinal_position) as toCols
from (
select distinct
  kcu.constraint_name as name,
  kcu.table_schema || '.' || kcu.table_name as fromTbl,
  rel_kcu.table_schema || '.' || rel_kcu.table_name as toTbl,
  kcu.column_name as fromCol,
  rel_kcu.column_name as toCol,
  kcu.ordinal_position
from information_schema.table_constraints tco
join information_schema.key_column_usage kcu on tco.constraint_schema = kcu.constraint_schema
  and tco.constraint_name = kcu.constraint_name
join information_schema.referential_constraints rco on tco.constraint_schema = rco.constraint_schema
  and tco.constraint_name = rco.constraint_name
join information_schema.key_column_usage rel_kcu on rco.unique_constraint_schema = rel_kcu.constraint_schema
  and rco.unique_constraint_name = rel_kcu.constraint_name
  and kcu.ordinal_position = rel_kcu.ordinal_position
where
  tco.constraint_type = 'FOREIGN KEY'
  and (
    (
      kcu.table_schema = '%s'
      and kcu.table_name = '%s'
    )
    or (
      rel_kcu.table_schema = '%s'
      and rel_kcu.table_name = '%s'
    )
  )
order by
  kcu.ordinal_position
) as rows
group by
  name,
  fromTbl,
  toTbl`, schemaName, tableName, schemaName, tableName)
	res, err := c.Query(query)
	if err != nil {
		return ret, err
	}
	for _, r := range res {
		rel = db.Relationship{
			Name:      r["name"].(string),
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

//GetIndexes get indexes for table
func (c *Conn) GetIndexes(schemaName, tableName string) ([]db.Index, error) {
	var ret = []db.Index{}
	var ind db.Index
	query := fmt.Sprintf(`
		select
			i.relname as index,
			array_to_string(array_agg(a.attname), ', ') as columns
		from
			pg_class t,
			pg_class i,
			pg_index ix,
			pg_attribute a,
			pg_indexes ind
		where
			t.oid = ix.indrelid
			and i.oid = ix.indexrelid
			and a.attrelid = t.oid
			and a.attnum = ANY(ix.indkey)
			and t.relkind = 'r'
			and ind.indexname=i.relname
			and ind.schemaname = '%s'
			and t.relname = '%s'
			and i.relname != concat(t.relname, '_pkey')
		group by
			t.relname,
			i.relname,
			ind.schemaname
		order by
			t.relname,
			i.relname;
	`, schemaName, tableName)
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
	if err != nil {
		return cols, err
	}
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
			if strings.Contains(col.DefaultValue, "::") {
				spl := strings.Split(col.DefaultValue, "::")
				if len(spl) > 0 {
					col.DefaultValue = spl[0]
				}
				col.DefaultValue = strings.Replace(col.DefaultValue, "'", "", -1)
			}
			col.Type = mapDataType(db.Interface2string(tp, false))
			if col.Type == "dbdate" {
				col.Length = 10
			}
			cols = append(cols, col)
		}
	}
	if len(cols) == 0 {
		return cols, errors.New("No columns found for " + schemaName + "." + tableName)
	}
	schemaCache[schemaName+"."+tableName] = cols
	return cols, nil
}

func mapDataType(dbType string) string {
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
		return "\"" + str + "\""
	}
	for _, item := range spl {
		if len(res) > 0 {
			res += sep
		}
		res += "\"" + strings.TrimSpace(item) + "\""
	}
	return res
}
