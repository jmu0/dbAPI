package api

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/handler"
	"github.com/jmu0/dbAPI/db"
)

var mutationConfig = graphql.ObjectConfig{
	Name:   "RootMutation",
	Fields: graphql.Fields{},
}
var queryConfig = graphql.ObjectConfig{
	Name:   "RootQuery",
	Fields: graphql.Fields{},
}
var err error

var dbm dbModel

var queryCache map[string]qCache
var mutex = &sync.RWMutex{}
var cacheExpire = time.Second * 30

//TODO: set read only

//BuildSchemaArgs provides arguments for buildschema function
type BuildSchemaArgs struct {
	//Tables list of schema.table for schema
	Tables []string
	Conn   db.Conn
}

//BuildSchema builds a schema from database
func BuildSchema(args BuildSchemaArgs) (graphql.Schema, error) {
	if len(args.Tables) == 0 { //get all db/table
		dbs, err := args.Conn.GetSchemaNames()
		if err != nil {
			return graphql.Schema{}, err
		}
		for _, db := range dbs {
			tbls, err := args.Conn.GetTableNames(db)
			if err != nil {
				return graphql.Schema{}, err
			}
			for _, tbl := range tbls {
				args.Tables = append(args.Tables, db+"."+tbl)
			}
		}
	}
	dbm = dbModel{
		tables: make(map[string]dbTable),
	}
	for _, tbl := range args.Tables {
		spl := strings.Split(tbl, ".")
		if len(spl) != 2 {
			return graphql.Schema{}, errors.New("Invalid table: " + tbl + ", valid tables are: <schema>.<table>")
		}
		table := dbTable{
			Name: tbl,
		}
		table.GetColumns(args.Conn)
		table.GetRelationships(args.Conn)
		table.BuildType()
		dbm.tables[table.Name] = table
	}
	for _, table := range dbm.tables {
		table.BuildRelationships(args.Conn)
		table.BuildQuery(args.Conn)
		table.BuildMutations(args.Conn)
	}

	return getSchema()
}

type qCache struct {
	time    time.Time
	results []map[string]interface{}
}

type dbModel struct {
	tables map[string]dbTable
}

func (m *dbModel) hasTable(name string) bool {
	for key := range m.tables {
		if strings.Replace(key, ".", "_", -1) == name {
			return true
		}
	}
	return false
}

type dbTable struct {
	Name          string
	Columns       []db.Column
	Relationships []db.Relationship
	Type          *graphql.Object
}

func (tbl *dbTable) getDbName() string {
	return strings.Split(tbl.Name, ".")[0]
}
func (tbl *dbTable) getTableName() string {
	return strings.Split(tbl.Name, ".")[1]
}

func (tbl *dbTable) getGqlName() string {
	return strings.Replace(tbl.Name, ".", "_", -1)
}

func (tbl *dbTable) GetColumns(c db.Conn) error {
	var err error
	tbl.Columns, err = c.GetColumns(tbl.getDbName(), tbl.getTableName())
	if err != nil {
		return err
	}
	return nil
}

func (tbl *dbTable) GetRelationships(c db.Conn) error {
	var err error
	tbl.Relationships, err = c.GetRelationships(tbl.getDbName(), tbl.getTableName())
	if err != nil {
		return err
	}
	return nil
}

func (tbl *dbTable) BuildType() {
	fields := graphql.Fields{}
	for _, col := range tbl.Columns {
		fields[col.Name] = &graphql.Field{
			Name: col.Name,
			Type: dbType2gqlType(col.Type),
		}
	}
	tbl.Type = graphql.NewObject(graphql.ObjectConfig{
		Name:   tbl.getGqlName(),
		Fields: fields,
	})
}

func (tbl *dbTable) BuildRelationships(c db.Conn) {
	for _, r := range tbl.Relationships {
		if r.Cardinality == "one-to-many" {
			relName := strings.Replace(r.FromTable, ".", "_", -1)
			if relTbl, ok := dbm.tables[r.FromTable]; ok {
				tbl.Type.AddFieldConfig(relName, &graphql.Field{
					Name:    relName,
					Type:    graphql.NewList(relTbl.Type),
					Resolve: resolveFuncOneToMany(r.FromTable, r.FromCols, r.ToCols, c),
				})
			}
		} else if r.Cardinality == "many-to-one" {
			relName := strings.Replace(r.ToTable, ".", "_", -1)
			if relTbl, ok := dbm.tables[r.ToTable]; ok {
				tbl.Type.AddFieldConfig(relName, &graphql.Field{
					Name:    relName,
					Type:    relTbl.Type,
					Resolve: resolveFuncManyToOne(r.ToTable, r.FromCols, r.ToCols, c),
				})
			}
		}
	}
}

func (tbl *dbTable) BuildQuery(c db.Conn) {
	args := graphql.FieldConfigArgument{}
	for _, p := range tbl.Columns {
		args[p.Name] = &graphql.ArgumentConfig{
			Type:         dbType2gqlType(p.Type),
			DefaultValue: "*",
		}
	}
	addQuery(&graphql.Field{
		Name:        tbl.getGqlName(),
		Type:        graphql.NewList(tbl.Type),
		Description: "Get " + tbl.getGqlName(),
		Args:        args,
		Resolve:     resolveFunc(tbl.getDbName(), tbl.getTableName(), tbl.Columns, c),
	})
}

var mutationResultType = graphql.NewObject(
	graphql.ObjectConfig{
		Name: "MutationResult",
		Fields: graphql.Fields{
			"last_insert_id": &graphql.Field{
				Type: graphql.Int,
			},
			"rows_affected": &graphql.Field{
				Type: graphql.Int,
			},
		},
	},
)

func (tbl *dbTable) BuildMutations(c db.Conn) {
	args := graphql.FieldConfigArgument{}
	for _, p := range tbl.Columns {
		args[p.Name] = &graphql.ArgumentConfig{
			Type: dbType2gqlType(p.Type),
		}
	}
	//add create mutation
	addMutation(&graphql.Field{
		Name:        "create_" + tbl.getGqlName(),
		Type:        mutationResultType,
		Description: "Create " + tbl.getGqlName(),
		Args:        args,
		Resolve:     resolveMutationCreate(tbl.getDbName(), tbl.getTableName(), tbl.Columns, c),
	})
	//add update mutation
	addMutation(&graphql.Field{
		Name:        "update_" + tbl.getGqlName(),
		Type:        mutationResultType,
		Description: "Update " + tbl.getGqlName(),
		Args:        args,
		Resolve:     resolveMutationUpdate(tbl.getDbName(), tbl.getTableName(), tbl.Columns, c),
	})
	//add delete mutation
	addMutation(&graphql.Field{
		Name:        "delete_" + tbl.getGqlName(),
		Type:        mutationResultType,
		Description: "Delete " + tbl.getGqlName(),
		Args:        args,
		Resolve:     resolveMutationDelete(tbl.getDbName(), tbl.getTableName(), tbl.Columns, c),
	})
}

func addMutation(field *graphql.Field) {
	mutationConfig.Fields.(graphql.Fields)[field.Name] = field
}

func addQuery(field *graphql.Field) {
	queryConfig.Fields.(graphql.Fields)[field.Name] = field
}

func getSchema() (graphql.Schema, error) {
	q := graphql.NewObject(queryConfig)
	var m *graphql.Object
	if len(mutationConfig.Fields.(graphql.Fields)) > 0 {
		m = graphql.NewObject(mutationConfig)
	}
	return graphql.NewSchema(graphql.SchemaConfig{
		Query:    q,
		Mutation: m,
	})
}

//HandleGQL serves graphql api
func HandleGQL(schema *graphql.Schema, w http.ResponseWriter, r *http.Request) {
	var query string
	start := time.Now()
	mutex.Lock()
	if queryCache == nil {
		queryCache = make(map[string]qCache)
	}
	mutex.Unlock()
	if r.Method == "GET" {
		query = r.URL.Query().Get("query")
	} else if r.Method == "POST" {
		err := r.ParseForm()
		if err == nil && len(r.Form) > 0 {
			query = r.Form.Get("query")
		} else {
			frm := make(map[string]string)
			err = json.NewDecoder(r.Body).Decode(&frm)
			if err == nil {
				query = frm["query"]
			}
		}
	}
	if len(query) == 0 {
		h := handler.New(&handler.Config{
			Schema:   schema,
			Pretty:   true,
			GraphiQL: true,
		})
		h.ServeHTTP(w, r)
		log.Println("Serving GraphiQL..")
		return
	}
	result := graphql.Do(graphql.Params{
		Schema:        *schema,
		RequestString: query,
	})
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	bytes, err := json.Marshal(result)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	bytes, err = compress(bytes)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Encoding", "gzip")
	w.Write(bytes)
	log.Println("Served graphql in", time.Since(start))
}

func dbType2gqlType(dbtype string) graphql.Type {
	dataTypes := map[string]graphql.Type{
		"string": graphql.String,
		"int":    graphql.Int,
		"float":  graphql.Float,
		"bool":   graphql.Boolean,
	}
	dbtype = strings.Split(dbtype, "(")[0]
	if tp, ok := dataTypes[dbtype]; ok {
		return tp
	}
	return graphql.String
}

func resolveMutationCreate(schemaName, tableName string, cols []db.Column, conn db.Conn) func(params graphql.ResolveParams) (interface{}, error) {
	return func(params graphql.ResolveParams) (interface{}, error) {
		args2cols(params.Args, cols)
		query, err := conn.InsertSQL(schemaName, tableName, cols)
		if err != nil {
			return nil, err
		}
		// log.Println("QUERY:", query)
		id, n, err := conn.Execute(query)
		if err != nil {
			return nil, err
		}
		if n == 0 {
			return nil, errors.New("No rows inserted")
		}
		deleteFromQueryCache(schemaName, tableName)
		return map[string]int64{
			"last_insert_id": id,
			"rows_affected":  n,
		}, nil
	}
}

func resolveMutationUpdate(schemaName, tableName string, cols []db.Column, conn db.Conn) func(params graphql.ResolveParams) (interface{}, error) {
	return func(params graphql.ResolveParams) (interface{}, error) {
		args2cols(params.Args, cols)
		query, err := conn.UpdateSQL(schemaName, tableName, cols)
		if err != nil {
			return nil, err
		}
		// log.Println("QUERY:", query)
		_, n, err := conn.Execute(query)
		if err != nil {
			return nil, err
		}
		if n == 0 {
			return nil, errors.New(schemaName + "_" + tableName + " not found")
		}
		deleteFromQueryCache(schemaName, tableName)
		return map[string]int64{
			"last_insert_id": -1,
			"rows_affected":  n,
		}, nil
	}
}

func resolveMutationDelete(schemaName, tableName string, cols []db.Column, conn db.Conn) func(params graphql.ResolveParams) (interface{}, error) {
	return func(params graphql.ResolveParams) (interface{}, error) {
		args2cols(params.Args, cols)
		query, err := conn.DeleteSQL(schemaName, tableName, cols)
		if err != nil {
			return nil, err
		}
		// log.Println("QUERY:", query)
		_, n, err := conn.Execute(query)
		if err != nil {
			return nil, err
		}
		if n == 0 {
			return nil, errors.New(schemaName + "_" + tableName + " not found")
		}
		deleteFromQueryCache(schemaName, tableName)
		return map[string]int64{
			"last_insert_id": -1,
			"rows_affected":  n,
		}, nil
	}
}

func resolveFunc(schemaName, tableName string, cols []db.Column, conn db.Conn) func(params graphql.ResolveParams) (interface{}, error) {
	return func(params graphql.ResolveParams) (interface{}, error) {
		var query string
		var res qCache
		var ok bool
		var err error
		query = "select * from " + conn.Quote(schemaName+"."+tableName)
		where := args2whereSQL(params.Args, cols, conn)
		if len(where) > 0 {
			query += " where" + where
		}
		mutex.RLock()
		if res, ok = queryCache[query]; ok {
			t := time.Now()
			if t.Sub(res.time) < cacheExpire {
				mutex.RUnlock()
				return res.results, nil
			}
		}
		mutex.RUnlock()
		// log.Println("DEBUG query resolve:", query)
		res.results, err = conn.Query(query)
		if err != nil {
			return res, err
		}
		res.time = time.Now()
		mutex.Lock()
		queryCache[query] = res
		mutex.Unlock()
		return res.results, nil
	}
}

func resolveFuncOneToMany(tbl, fromcols, tocols string, conn db.Conn) func(params graphql.ResolveParams) (interface{}, error) {
	return func(params graphql.ResolveParams) (interface{}, error) {
		var query, where string
		var res qCache
		var err error
		var ok bool
		var toCols []string

		query = "select * from " + conn.Quote(tbl) + " where "
		toCols = strings.Split(tocols, ", ")
		for i, c := range strings.Split(fromcols, ", ") {
			if param, ok := params.Source.(map[string]interface{}); ok {
				if val, ok := param[toCols[i]]; ok {
					if len(where) > 0 {
						where += " and "
					}
					where += conn.Quote(c) + "='" + db.Escape(val.(string)) + "'"
				}
			}
		}
		query += where
		mutex.RLock()
		if res, ok = queryCache[query]; ok {
			t := time.Now()
			if t.Sub(res.time) < cacheExpire {
				// log.Println("QUERY FROM CACHE (one to many):", query)
				mutex.RUnlock()
				return res.results, nil
			}
		}
		mutex.RUnlock()
		// log.Println("DEBUG query one to many:", query)
		res.results, err = conn.Query(query)
		if err != nil {
			return res, err
		}
		res.time = time.Now()
		mutex.Lock()
		queryCache[query] = res
		mutex.Unlock()
		// log.Println("QUERY:", query)
		return res.results, nil
	}
}

func resolveFuncManyToOne(tbl, fromCols, toCols string, conn db.Conn) func(params graphql.ResolveParams) (interface{}, error) {
	return func(params graphql.ResolveParams) (interface{}, error) {
		var query, where string
		var res qCache
		var err error
		var ok bool
		query = "select * from " + conn.Quote(tbl) + " where "
		fcSplit := strings.Split(fromCols, ", ")
		for i, c := range strings.Split(toCols, ", ") {
			if param, ok := params.Source.(map[string]interface{}); ok {
				if val, ok := param[fcSplit[i]]; ok {
					if len(where) > 0 {
						where += " and "
					}
					where += conn.Quote(c) + "='" + db.Escape(val.(string)) + "'"
				}
			}
		}
		query += where
		mutex.RLock()
		if res, ok = queryCache[query]; ok {
			t := time.Now()
			if t.Sub(res.time) < cacheExpire {
				// log.Println("QUERY FROM CACHE (many to one):", query)
				mutex.RUnlock()
				return res.results[0], nil
			}
		}
		mutex.RUnlock()
		// log.Println("DEBUG query many to one:", query)
		res.results, err = conn.Query(query)
		if err != nil {
			return nil, err
		}
		if len(res.results) == 0 {
			return nil, errors.New("Not found " + tbl + " " + fromCols + " / " + toCols)
		}
		res.time = time.Now()
		mutex.Lock()
		queryCache[query] = res
		mutex.Unlock()
		// log.Println("QUERY:", query)
		return res.results[0], nil
	}
}

func args2whereSQL(args map[string]interface{}, cols []db.Column, conn db.Conn) string {
	var ret, orval string
	var index int
	for key, value := range args {
		if val, ok := value.(string); val != "*" {
			index = findColIndex(key, cols)
			if index > -1 {
				if len(ret) > 0 {
					ret += " and"
				}
				if ok && strings.Contains(value.(string), "*") {
					ret += " " + conn.Quote(key) + " like '" + db.Escape(strings.Replace(value.(string), "*", "%", -1)) + "'"
				} else {
					switch value.(type) {
					case int:
						ret += " " + conn.Quote(key) + "=" + strconv.Itoa(value.(int))
					case bool:
						if value.(bool) == true {
							ret += " " + conn.Quote(key) + "=1"
						} else {
							ret += " " + conn.Quote(key) + "=0"
						}
					default:
						val = db.Escape(val)
						if len(val) > 2 && val[:2] == ">=" {
							ret += " " + conn.Quote(key) + " >= '" + val[2:] + "' "
						} else if len(val) > 2 && val[:2] == "<=" {
							ret += " " + conn.Quote(key) + " <= '" + val[2:] + "' "
						} else if len(val) > 2 && val[:2] == "<>" {
							ret += " " + conn.Quote(key) + " <> '" + val[2:] + "' "
						} else if len(val) > 1 && val[:1] == ">" {
							ret += " " + conn.Quote(key) + " > '" + val[1:] + "' "
						} else if len(val) > 1 && val[:1] == "<" {
							ret += " " + conn.Quote(key) + " < '" + val[1:] + "' "
						} else if len(val) > 1 && val[:1] == "!" {
							ret += " " + conn.Quote(key) + " <> '" + val[1:] + "' "
						} else if len(val) > 3 && strings.Contains(val, "||") {
							for _, o := range strings.Split(val, "||") {
								if len(o) > 0 {
									if len(orval) > 0 {
										orval += " or "
									}
									orval += conn.Quote(key) + " = '" + o + "' "
								}
							}
							if len(orval) > 0 {
								ret += " (" + orval + ")"
							}
						} else {
							ret += " " + conn.Quote(key) + " = '" + val + "' "
						}
					}
				}
			}
		}
	}
	return ret
}

func args2cols(args map[string]interface{}, cols []db.Column) {
	for i, c := range cols {
		if a, ok := args[c.Name]; ok {
			cols[i].Value = a
		}
	}
}

func deleteFromQueryCache(schemaName, tableName string) {
	for q := range queryCache {
		if strings.Contains(q, schemaName) && strings.Contains(q, tableName) {
			mutex.Lock()
			delete(queryCache, q)
			mutex.Unlock()
		}
	}
}
