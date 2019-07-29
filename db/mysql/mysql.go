package mysql

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	//used for connecting to datbase
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmu0/settings"
)

//TODO create interface, handle different db's (mysql, postgres)

//returns connection string for database driver
func makeDSN(server, user, password string) string {
	var port string
	port = "3306"
	return user + ":" + password + "@tcp(" + server + ":" + port + ")/"
}

//Connect connect to database
func Connect(arg ...string) (*sql.DB, error) {
	//TODO change path
	var path string
	//TODO save login somewhere else
	path = "orm.conf"
	if _, err := os.Stat(path); os.IsNotExist(err) {
		path = "/etc/orm.conf"
	}
	set := settings.Settings{File: path}
	database, err := set.Get("database")
	if err != nil {
		fmt.Println(err)
		fmt.Printf("Database server: ")
		fmt.Scanln(&database)
	}
	usr, err := set.Get("user")
	if err != nil {
		fmt.Println(err)
		fmt.Printf("Username: ")
		fmt.Scanln(&usr)
	}
	pwd, err := set.Get("password")
	if err != nil {
		fmt.Println(err)
		fmt.Printf("Password: ")
		fmt.Scanln(&pwd)
	}
	db, err := sql.Open("mysql", makeDSN(database, usr, pwd))
	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(0)
	d, _ := time.ParseDuration("1 second")
	db.SetConnMaxLifetime(d)
	if err != nil {
		return db, err
	}
	return db, nil
}

//DoQuery connects, queries and returns results
func DoQuery(query string) ([]map[string]interface{}, error) {
	var err error
	db, err := Connect()
	ret := make([]map[string]interface{}, 0)
	if err != nil {
		return ret, err
	}
	defer db.Close()
	ret, err = Query(db, query)
	if err != nil {
		return ret, err
	}
	return ret, nil
}

//Query Get slice of map[string]interface{} from database
func Query(db *sql.DB, query string) ([]map[string]interface{}, error) {
	res := make([]map[string]interface{}, 0)
	rows, err := db.Query(query)
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
	//DEBUG:log.Println(res)
	return res, nil
}

//ServeQuery does query and writes json to responseWriter
func ServeQuery(query string, w http.ResponseWriter) error {
	result, err := DoQuery(query)
	if err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return err
	}
	json, err := json.Marshal(result)
	if err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return err
	}
	// log.Println("GET in bestelling voor", lokatie)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write(json)
	return nil
}

func getColsWithValues(db *sql.DB, dbName string, tblName string, r *http.Request) []Column {
	cols := GetColumns(db, dbName, tblName)
	data, err := getRequestData(r)
	if err != nil {
		log.Println("REST: ERROR: POST:", dbName, tblName, err)
	}

	//set column values
	values2columns(&cols, data)
	return cols
}

func values2columns(cols *[]Column, values map[string]interface{}) {
	for key, value := range values {
		index := findColIndex(key, *cols)
		if index > -1 {
			(*cols)[index].Value = Escape(value.(string))
		}
	}
}

func cols2json(table string, cols []Column) ([]byte, error) {
	var ret map[string]interface{}
	ret = make(map[string]interface{})
	ret["type"] = table
	for _, col := range cols {
		ret[col.Field] = col.Value
	}
	json, err := json.Marshal(ret)
	if err != nil {
		return []byte(""), err
	}
	return json, nil
}

func findColIndex(field string, cols []Column) int {
	for index, col := range cols {
		if col.Field == field {
			return index
		}
	}
	return -1
}

func writeQueryResults(db *sql.DB, q string, w http.ResponseWriter) {
	var ret interface{}
	res, err := Query(db, q)
	//fmt.Println("REST: DEBUG: writeQueryResults:", q)
	if err != nil {
		http.Error(w, "No results found", http.StatusNotFound)
		return
	}
	if len(res) == 1 {
		ret = res[0]
	} else {
		ret = res
	}
	bytes, err := json.Marshal(ret)
	if err != nil {
		fmt.Println("HandleRest: error encoding json:", err)
		http.Error(w, "No results found", http.StatusNotFound)
		return
	}
	//drop password fields
	var pwReg = ",\"?([P,p]ass[W,w]o?r?d|[W,w]acht[W,w]o?o?r?d?)\"?:\"(.*?)\""
	passwdReg := regexp.MustCompile(pwReg)
	str := passwdReg.ReplaceAllString(string(bytes), "")
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write([]byte(str))
}

//getRequestData get data from post request
func getRequestData(req *http.Request) (map[string]interface{}, error) {
	err := req.ParseForm()
	if err != nil {
		return make(map[string]interface{}), err
	}
	res := make(map[string]interface{})
	for k, v := range req.Form {
		res[k] = strings.Join(v, "")
	}
	return res, nil
}

//DbObject interface
type DbObject interface {
	//TODO move to different file
	GetDbInfo() (dbName string, tblName string)
	GetColumns() []Column
	Get(key string) (Column, error)
	Set(key string, value interface{}) error
	Save() (Nr int, err error)
	Delete() (Nr int, err error)
}

//Escape string to prevent common sql injection attacks
func Escape(str string) string {
	//TODO move to different file
	// ", ', 0=0
	str = strings.Replace(str, "\"", "\\\"", -1)
	str = strings.Replace(str, "''", "'", -1)
	str = strings.Replace(str, "'", "''", -1)

	// \x00, \n, \r, \ and \x1a"
	str = strings.Replace(str, "\x00", "", -1)
	str = strings.Replace(str, "\n", "", -1)
	str = strings.Replace(str, "\r", "", -1)
	str = strings.Replace(str, "\x1a", "", -1)

	//multiline attack
	str = strings.Replace(str, ";", " ", -1)

	//comments attack
	str = strings.Replace(str, "--", "", -1)
	str = strings.Replace(str, "#", "", -1)
	str = strings.Replace(str, "/*", "", -1)
	str = strings.Replace(str, "*/", "", -1)
	return str
}

//save can be used by HandleREST and DbObject
func save(dbName string, tblName string, cols []Column) (int, int, error) {
	var err error
	db, err := Connect()
	if err != nil {
		return -1, -1, err
	}
	defer db.Close()

	query := "insert into " + dbName + "." + tblName + " "
	fields := "("
	strValues := "("
	insValues := make([]interface{}, 0)
	updValues := make([]interface{}, 0)
	strUpdate := ""
	for _, c := range cols {
		//log.Println("DEBUG:", c)
		if c.Value != nil {
			if (GetType(c.Type) == "int" && c.Value == "") == false { //skip auto_increment column
				if len(fields) > 1 {
					fields += ", "
				}
				fields += c.Field
				if len(strValues) > 1 {
					strValues += ", "
				}
				strValues += "?"
				insValues = append(insValues, c.Value)
				if len(strUpdate) > 0 {
					strUpdate += ", "
				}
				strUpdate += c.Field + "=?"
				updValues = append(updValues, c.Value)
			}
		}
	}
	fields += ")"
	strValues += ")"
	query += fields + " values " + strValues
	query += " on duplicate key update " + strUpdate
	// log.Println("DEBUG SAVE query:", query)
	insValues = append(insValues, updValues...)
	qr, err := db.Exec(query, insValues...)
	// stmt, err := db.Prepare(query)
	// if err != nil {
	// 	return -1, -1, err
	// }
	// qr, err := stmt.Exec(insValues...)
	if err != nil {
		return -1, -1, err
	}

	id, err := qr.LastInsertId()
	if err != nil {
		id = -1
	}
	n, err := qr.RowsAffected()
	if err != nil {
		n = -1
	}
	// fmt.Println("REST: DEBUG: save result n:", n, "id:", id)
	return int(n), int(id), nil
}

//delete can be used by HandleREST and DbObject
func delete(dbName string, tblName string, cols []Column) (int, error) {
	db, err := Connect()
	if err != nil {
		return 1, err
	}
	defer db.Close()
	query := "delete from " + dbName + "." + tblName + " where"
	where, err := StrPrimaryKeyWhereSQL(cols)
	if err != nil {
		fmt.Println("error:", err)
	}
	query += where
	res, err := db.Exec(query)
	if err != nil {
		return 1, err
	}
	nrrows, _ := res.RowsAffected()
	if nrrows < 1 {
		return 1, errors.New("No rows deleted")
	}
	return 0, nil
}

//return string for query for value
func valueString(val interface{}) string {
	var value string
	if val == nil {
		return ""
	}
	switch t := val.(type) {
	case string:
		value += "\"" + Escape(val.(string)) + "\""
	case int, int32, int64:
		value += strconv.Itoa(val.(int))
	default:
		fmt.Println(t)
		value += "\"" + Escape(val.(string)) + "\""
	}
	return value
}

//GetDatabaseNames Get database names from server
func GetDatabaseNames(db *sql.DB) []string {
	dbs := []string{}
	query := "show databases"
	rows, err := db.Query(query)
	defer rows.Close()
	if err == nil && rows != nil {
		dbName := ""
		for rows.Next() {
			rows.Scan(&dbName)
			if !skipDb(dbName) {
				dbs = append(dbs, dbName)
			}
		}
	}
	return dbs
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

//GetTableNames Get table names from database
func GetTableNames(db *sql.DB, dbName string) []string {
	tbls := []string{}
	query := "show tables in " + dbName
	rows, err := db.Query(query)
	if err != nil {
		return tbls
	} else if rows != nil {
		tableName := ""
		for rows.Next() {
			rows.Scan(&tableName)
			tbls = append(tbls, tableName)
		}
	}
	defer rows.Close()
	return tbls
}

//GetColumns get list of columns from database table
func GetColumns(db *sql.DB, dbName string, tableName string) []Column {
	cols := []Column{}
	var col Column
	query := "show columns from " + dbName + "." + tableName
	//TODO: waarom zie ik geen auto_increment in kolom Extra??
	rows, err := db.Query(query)
	defer rows.Close()
	if err == nil && rows != nil {
		for rows.Next() {
			col = Column{}
			rows.Scan(&col.Field, &col.Type, &col.Null, &col.Key, &col.Default, &col.Extra)
			// fmt.Println("DEBUG:",rows)
			// fmt.Println("DEBUG GetColumns:", col)
			cols = append(cols, col)
		}
	}
	return cols
}

//Column Structure to represent table column
type Column struct {
	Field   string
	Type    string
	Null    string
	Key     string
	Default string
	Extra   string
	Value   interface{}
}

//GetRelationships gets relationships for table
func GetRelationships(db *sql.DB, dbName string, tableName string) ([]Relationship, error) {
	var ret []Relationship
	var query = `select concat(table_schema, ".", table_name) as fromTbl, 
			group_concat(column_name separator ", ") as fromCols,
			concat(referenced_table_schema, ".", referenced_table_name) as toTbl, 
			group_concat(referenced_column_name separator ", ") as toCols
			from (select constraint_name, table_schema,table_name,column_name,referenced_table_schema,referenced_table_name, 
			referenced_column_name from information_schema.key_column_usage
			where (referenced_table_schema="` + dbName + `" and referenced_table_name="` + tableName + `") 
			or (table_schema="` + dbName + `" and table_name="` + tableName + `")
			and constraint_name <> "PRIMARY"
			) as relations group by fromTbl, toTbl`
	// log.Println("DEBUG:", query)
	res, err := Query(db, query)
	if err != nil {
		return ret, err
	}
	for _, r := range res {
		var rel = Relationship{
			FromTable: r["fromTbl"].(string),
			FromCols:  r["fromCols"].(string),
			ToTable:   r["toTbl"].(string),
			ToCols:    r["toCols"].(string),
		}
		if rel.FromTable == dbName+"."+tableName {
			rel.Cardinality = "many-to-one"
		} else if rel.ToTable == dbName+"."+tableName {
			rel.Cardinality = "one-to-many"
		}
		ret = append(ret, rel)
	}
	return ret, nil
}

//Relationship between tables
type Relationship struct {
	FromTable   string
	FromCols    string
	ToTable     string
	ToCols      string
	Cardinality string
}

//GetType find out go data type for database data type
func GetType(t string) string {
	//TODO: more datatypes
	var dataTypes map[string]string
	dataTypes = map[string]string{
		"varchar":  "string",
		"tinyint":  "int",
		"smallint": "int",
		"datetime": "string",
		"int":      "int",
	}
	t = strings.Split(t, "(")[0]
	if tp, ok := dataTypes[t]; ok {
		return tp
	}
	return "string"
}

func setAutoIncColumn(id int, cols []Column) []Column {
	//fmt.Println("DEBUG:setAutoIncColumn")
	for index, col := range cols {
		if strings.Contains(col.Type, "int") && col.Key == "PRI" {
			//fmt.Println("DEBUG:found", col.Field)
			cols[index].Value = id
		}
	}
	return cols
}

//StrPrimaryKeyWhereSQL returns where part of query
func StrPrimaryKeyWhereSQL(cols []Column) (string, error) {
	var ret string
	for _, c := range cols {
		if c.Key == "PRI" {
			if len(ret) > 0 {
				ret += " and"
			}
			ret += " " + c.Field + " = " + valueString(c.Value)
		}
	}
	if len(ret) == 0 {
		return "", errors.New("Primary key not found (StrPrimaryKeyWhereSQL")
	}
	return ret, nil
}

//PrimaryKeyCols filters primary key columns from []Column
func PrimaryKeyCols(cols []Column) []Column {
	var ret []Column
	for _, c := range cols {
		if c.Key == "PRI" {
			ret = append(ret, c)
		}
	}
	return ret
}
