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

// var types map[string]*graphql.Object //TODO nog nodig? custom types?
var mutationConfig = graphql.ObjectConfig{
	Name:   "RootMutation",
	Fields: graphql.Fields{},
}
var queryConfig = graphql.ObjectConfig{
	Name:   "RootQuery",
	Fields: graphql.Fields{},
}
var err error

// var conn *sql.DB
var dbm dbModel

var queryCache map[string]qCache
var mutex = &sync.RWMutex{}

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

// func (m *dbModel) BuildSchema() (graphql.Schema, error) {
// 	for _, tbl := range m.tables {
// 		log.Println("DEBUG adding type", tbl.Name, tbl.GqlType)
// 		addType(tbl.Type)
// 		log.Println("DEBUG adding query", tbl.Name, tbl.GqlQuery)
// 		addQuery(tbl.GqlQuery)
// 		for _, mut := range tbl.GqlMutations {
// 			log.Println("DEBUG adding Mutation", tbl.Name, mut)
// 			addMutation(mut)
// 		}
// 	}
// 	return getSchema()
// }

// func (t *dbModel) createTypes() {

// }

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
		// log.Println("DEBUG rel", r)
		if r.Cardinality == "one-to-many" {
			relName := strings.Replace(r.FromTable, ".", "_", -1)
			// log.Println("DEBUG one to many", relName)
			// if dbm.hasTable(relName) {
			// log.Println("DEBUG tables:", dbm.tables)
			// log.Println("DEBUG:", relName, dbm.tables[relName])
			if relTbl, ok := dbm.tables[r.FromTable]; ok {
				// if _, ok := types[relName]; !ok {
				// addTableToSchema(r.FromTable)
				// log.Println("DEBUG HAAAA!", tbl)
				// }
				tbl.Type.AddFieldConfig(relName, &graphql.Field{
					Name:    relName,
					Type:    graphql.NewList(relTbl.Type),
					Resolve: resolveFuncOneToMany(r.FromTable, r.FromCols, c),
				})
				// log.Println("DEBUG: added relationship", relName, "to", tbl.Name)
				// } else {
				// log.Println("DEBUG skip relationship", relName)
			}
		} else if r.Cardinality == "many-to-one" {
			relName := strings.Replace(r.ToTable, ".", "_", -1)
			// if dbm.hasTable(relName) {
			if relTbl, ok := dbm.tables[r.ToTable]; ok {
				// log.Println("DEBUGmanytoone", relName, types[relName])
				// if _, ok := types[relName]; !ok {
				// addTableToSchema(r.ToTable)
				// log.Println("DEBUG HAAAA! many-to-one", tbl)
				// }
				tbl.Type.AddFieldConfig(relName, &graphql.Field{
					Name:    relName,
					Type:    relTbl.Type,
					Resolve: resolveFuncManyToOne(r.ToTable, r.FromCols, r.ToCols, c),
				})
				// log.Println("DEBUG: added relationship", relName, "to", tbl.Name)
				// } else {
				// log.Println("DEBUG: skipping", relName)
			}
		}
	}
	// log.Println("DEBUG built relationships for", tbl.Name)
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
	// log.Println("DEBUG:built query", tbl.Name)
}

func (tbl *dbTable) BuildMutations() {
	//MUTATION
	/*
		addMutation(&graphql.Field{
			Name:        "createTodo",
				Type:        types["Todo"], // the return type for this field
				Description: "Create new todo",
				Args: graphql.FieldConfigArgument{
					"text": &graphql.ArgumentConfig{
						Type: graphql.NewNonNull(graphql.String),
					},
				},
				Resolve: func(params graphql.ResolveParams) (interface{}, error) {
					return map[string]string{
						"test": "een",
						"twee": "drie",
						}, nil
					},
				})
				//*/

}

// func addType(newType *graphql.Object) {
// 	if types == nil {
// 		types = make(map[string]*graphql.Object)
// 	}
// 	types[newType.Name()] = newType
// }

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
	//TODO: compress results
	var query string
	start := time.Now()
	mutex.Lock()
	if queryCache == nil {
		queryCache = make(map[string]qCache)
	}
	mutex.Unlock()
	// log.Println("DEBUG:", r.Method)
	if r.Method == "GET" {
		query = r.URL.Query().Get("query")
	} else if r.Method == "POST" {
		err := r.ParseForm()
		// log.Println("DEBUG Form:", r.Form)
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
	// log.Println("DEBUG query:", query)
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
	// log.Println("GQL query:", query)
	result := graphql.Do(graphql.Params{
		Schema:        *schema,
		RequestString: query, //TODO: use GET or POST
	})
	// log.Println("DEBUG:queryCache:", len(queryCache))
	log.Println("Served graphql in", time.Since(start))
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(result)
}

//BuildSchemaArgs provides arguments for buildschema function
type BuildSchemaArgs struct {
	//Tables list of schema.table for schema
	Tables []string
}

//BuildSchema builds a schema from database
func BuildSchema(args BuildSchemaArgs, c db.Conn) (graphql.Schema, error) {
	if err != nil { //connection error
		return graphql.Schema{}, err
	}
	if len(args.Tables) == 0 { //get all db/table
		dbs, err := c.GetSchemaNames()
		if err != nil {
			return graphql.Schema{}, err
		}
		for _, db := range dbs {
			tbls, err := c.GetTableNames(db)
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
		table.GetColumns(c)
		table.GetRelationships(c)
		table.BuildType()
		// if _, ok := types[strings.Replace(tbl, ".", "_", -1)]; !ok {
		// err := addTableToSchema(tbl)
		// if err != nil {
		// 	return graphql.Schema{}, err
		// }
		// // } else {
		// log.Println("DEBUG", tbl, "is er al")
		// }
		dbm.tables[table.Name] = table
	}
	for _, table := range dbm.tables {
		table.BuildRelationships(c)
		table.BuildQuery(c)
		table.BuildMutations()
	}

	return getSchema()
}

// func addTableToSchema(tbl string) error {
// log.Println("Creating schema for", tbl+"...")

// cols := GetColumns(conn, spl[0], spl[1])
// name := strings.Replace(tbl, ".", "_", -1)
// rel, err := GetRelationships(conn, spl[0], spl[1])
// if err != nil {
// log.Println("ERROR: get table relationships:", err)
// }

//relationships
// for _, r := range rel {
// 	// log.Println("DEBUG rel", r)
// 	if r.Cardinality == "one-to-many" {
// 		relName := strings.Replace(r.FromTable, ".", "_", -1)
// 		// log.Println("DEBUG one to many", relName)
// 		if checkTables.hasTable(relName) {
// 			// if _, ok := types[relName]; !ok {
// 			// addTableToSchema(r.FromTable)
// 			// log.Println("DEBUG HAAAA!", tbl)
// 			// }
// 			fields[relName] = &graphql.Field{
// 				Name:    relName,
// 				Type:    graphql.NewList(types[relName]),
// 				Resolve: resolveFuncOneToMany(r.FromTable, r.FromCols),
// 			}
// 			// } else {
// 			// log.Println("DEBUG skip relationship", relName)
// 		}
// 	} else if r.Cardinality == "many-to-one" {
// 		relName := strings.Replace(r.ToTable, ".", "_", -1)
// 		if checkTables.hasTable(relName) {
// 			// log.Println("DEBUGmanytoone", relName, types[relName])
// 			// if _, ok := types[relName]; !ok {
// 			// addTableToSchema(r.ToTable)
// 			// log.Println("DEBUG HAAAA! many-to-one", tbl)
// 			// }
// 			fields[relName] = &graphql.Field{
// 				Name:    relName,
// 				Type:    types[relName],
// 				Resolve: resolveFuncManyToOne(r.FromTable, r.FromCols),
// 			}
// 		} else {
// 			log.Println("DEBUG: skipping", relName)
// 		}
// 	}
// }
// newType := graphql.NewObject(graphql.ObjectConfig{
// 	Name:   name,
// 	Fields: fields,
// })
// addType(newType)

// //QUERY
// //*
// args := graphql.FieldConfigArgument{}
// // pri := PrimaryKeyCols(cols)
// for _, p := range cols {
// 	args[p.Field] = &graphql.ArgumentConfig{
// 		Type:         dbType2gqlType(p.Type),
// 		DefaultValue: "*",
// 	}
// }
// //*/
// addQuery(&graphql.Field{
// 	Name:        name,
// 	Type:        graphql.NewList(types[name]),
// 	Description: "Get " + tbl,
// 	Args:        args,
// 	Resolve:     resolveFunc(spl[0], spl[1], cols),
// })

//MUTATION
/*
	addMutation(&graphql.Field{
		Name:        "createTodo",
		Type:        types["Todo"], // the return type for this field
		Description: "Create new todo",
		Args: graphql.FieldConfigArgument{
			"text": &graphql.ArgumentConfig{
				Type: graphql.NewNonNull(graphql.String),
			},
		},
		Resolve: func(params graphql.ResolveParams) (interface{}, error) {
			return map[string]string{
				"test": "een",
				"twee": "drie",
			}, nil
		},
	})
	//*/
// return nil
// }

func dbType2gqlType(dbtype string) graphql.Type {
	//TODO: more datatypes
	dataTypes := map[string]graphql.Type{
		"varchar":  graphql.String,
		"tinyint":  graphql.Int,
		"smallint": graphql.Int,
		"datetime": graphql.String,
		"int":      graphql.Int,
	}
	dbtype = strings.Split(dbtype, "(")[0]
	if tp, ok := dataTypes[dbtype]; ok {
		return tp
	}
	return graphql.String
}

func resolveFunc(schemaName, tableName string, cols []db.Column, conn db.Conn) func(params graphql.ResolveParams) (interface{}, error) {
	return func(params graphql.ResolveParams) (interface{}, error) {
		var query string
		var res qCache
		var ok bool
		var err error
		query = "select * from " + schemaName + "." + tableName
		where := args2whereSQL(params.Args, cols)
		if len(where) > 0 {
			query += " where" + where
		}
		mutex.RLock()
		if res, ok = queryCache[query]; ok {
			t := time.Now()
			if t.Sub(res.time) < time.Second*10 {
				// log.Println("QUERY FROM CACHE:", query)
				mutex.RUnlock()
				return res.results, nil
			}
		}
		mutex.RUnlock()
		// log.Println("QUERY:", query)
		res.results, err = db.Query(conn, query)
		if err != nil {
			return res, err
		}
		// log.Println("DEBUG results", res.results)
		res.time = time.Now()
		mutex.Lock()
		queryCache[query] = res
		mutex.Unlock()
		return res.results, nil
	}
}

func resolveFuncOneToMany(tbl, cols string, conn db.Conn) func(params graphql.ResolveParams) (interface{}, error) {
	return func(params graphql.ResolveParams) (interface{}, error) {
		var query, where string
		var res qCache
		var err error
		var ok bool

		query = "select * from " + tbl + " where "
		for _, c := range strings.Split(cols, ", ") {
			// log.Println("DEBUG: cols", c)
			if param, ok := params.Source.(map[string]interface{}); ok {
				if val, ok := param[c]; ok {
					if len(where) > 0 {
						where += " and "
					}
					where += c + "='" + db.Escape(val.(string)) + "'"
				}
			}
		}
		query += where
		mutex.RLock()
		if res, ok = queryCache[query]; ok {
			t := time.Now()
			if t.Sub(res.time) < time.Second*10 {
				log.Println("QUERY FROM CACHE:", query)
				mutex.RUnlock()
				return res.results, nil
			}
		}
		mutex.RUnlock()

		res.results, err = db.Query(conn, query)
		if err != nil {
			return res, err
		}
		res.time = time.Now()
		mutex.Lock()
		queryCache[query] = res
		mutex.Unlock()
		log.Println("QUERY:", query)
		return res.results, nil
	}
}

func resolveFuncManyToOne(tbl, fromCols, toCols string, conn db.Conn) func(params graphql.ResolveParams) (interface{}, error) {
	return func(params graphql.ResolveParams) (interface{}, error) {
		var query, where string
		var res qCache
		var err error
		var ok bool
		query = "select * from " + tbl + " where "
		fcSplit := strings.Split(fromCols, ", ")
		for i, c := range strings.Split(toCols, ", ") {
			// log.Println("DEBUG: cols", c)
			if param, ok := params.Source.(map[string]interface{}); ok {
				if val, ok := param[fcSplit[i]]; ok {
					if len(where) > 0 {
						where += " and "
					}
					where += c + "='" + db.Escape(val.(string)) + "'"
				}
			}
		}
		query += where
		mutex.RLock()
		if res, ok = queryCache[query]; ok {
			t := time.Now()
			if t.Sub(res.time) < time.Second*10 {
				log.Println("QUERY FROM CACHE (many to one):", query)
				mutex.RUnlock()
				log.Println("DEBUG results query from cache:", res.results)
				return res.results, nil
			}
		}
		mutex.RUnlock()
		res.results, err = db.Query(conn, query)
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
		log.Println("QUERY:", query)

		return res.results[0], nil
	}
}

func args2whereSQL(args map[string]interface{}, cols []db.Column) string {
	var ret string
	var index int
	for key, value := range args {
		if val, ok := value.(string); val != "*" {
			index = findColIndex(key, cols)
			if index > -1 {
				if len(ret) > 0 {
					ret += " and"
				}
				if ok && strings.Contains(value.(string), "*") {
					ret += " " + key + " like '" + db.Escape(strings.Replace(value.(string), "*", "%", -1)) + "'"
				} else {
					switch value.(type) {
					case int:
						ret += " " + key + "=" + strconv.Itoa(value.(int))
					case bool:
						if value.(bool) == true {
							ret += " " + key + "=1"
						} else {
							ret += " " + key + "=0"
						}
					default:
						ret += " " + key + "='" + db.Escape(value.(string)) + "'"
					}
				}
			}
		}
	}
	return ret
}

/*
//TestSchema gets a test schema
func TestSchema() (graphql.Schema, error) { //TODO remove this function
	addType(graphql.NewObject(graphql.ObjectConfig{
		Name: "Todo",
		Fields: graphql.Fields{
			"id": &graphql.Field{
				Type: graphql.String,
			},
			"text": &graphql.Field{
				Type: graphql.String,
			},
			"done": &graphql.Field{
				Type: graphql.Boolean,
			},
		},
	}))

	addMutation(&graphql.Field{
		Name:        "createTodo",
		Type:        types["Todo"], // the return type for this field
		Description: "Create new todo",
		Args: graphql.FieldConfigArgument{
			"text": &graphql.ArgumentConfig{
				Type: graphql.NewNonNull(graphql.String),
			},
		},
		Resolve: func(params graphql.ResolveParams) (interface{}, error) {
			return map[string]string{
				"test": "een",
				"twee": "drie",
			}, nil
		},
	})

	addQuery(&graphql.Field{
		Name:        "todo",
		Type:        types["Todo"],
		Description: "Get single todo",
		Args: graphql.FieldConfigArgument{
			"id": &graphql.ArgumentConfig{
				Type: graphql.String,
			},
		},
		Resolve: func(params graphql.ResolveParams) (interface{}, error) {
			return map[string]string{
				"id":   "1",
				"text": "dit is een test",
				"done": "false",
			}, nil
		},
	})
	return getSchema()
}
//*/
