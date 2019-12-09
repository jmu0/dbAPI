package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/jmu0/dbAPI/api"
	"github.com/jmu0/dbAPI/db"

	"github.com/jmu0/dbAPI/db/mysql"
	"github.com/jmu0/dbAPI/db/postgresql"
)

var listenAddr = ":8282"

func main() {
	// testMysql()
	// testPostgres()
	// runAPIServer()
	testGraphql()
	// testGetSQL()
	// testDb2Yml()
	//testYml2Db()
	// testSort()
}

func testPostgres() {
	pg, err := connectTestPostgres()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("\nSchema names in database:")
	fmt.Println(pg.GetSchemaNames())
	fmt.Println("\nTables in public:")
	fmt.Println(pg.GetTableNames("public"))
	fmt.Println("\nGet columns for tbl1:")
	cols, err := pg.GetColumns("public", "tbl1")
	printdbcols(cols)
	c, err := pg.GetColumns("assortiment", "plant")
	printdbcols(c)
	fmt.Println("\nRelationships for assortiment.artikel:")
	fmt.Println(pg.GetRelationships("assortiment", "artikel"))
}

func connectTestMysql() (db.Conn, error) {
	var d = mysql.Conn{}
	err := d.Connect(map[string]string{
		"hostname": "jos-desktop",
		"username": "web",
		"password": "jmu0!",
	})
	if err != nil {
		return nil, err
	}
	return &d, nil
}
func connectTestPostgres() (db.Conn, error) {
	var d = postgresql.Conn{}
	err := d.Connect(map[string]string{
		"hostname": "jos-desktop",
		"username": "jos",
		"password": "jmu0!",
		"database": "test",
	})
	if err != nil {
		return nil, err
	}
	return &d, nil
}

func testMysql() {
	d, err := connectTestMysql()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("\nDatabases on server:")
	fmt.Println(d.GetSchemaNames())
	fmt.Println("\nTables in assortiment:")
	fmt.Println(d.GetTableNames("Assortiment"))
	fmt.Println("\nColumns in Verkoop.Orderregels:")
	fmt.Println(d.GetColumns("Verkoop", "Orderregels"))
	c, _ := d.GetColumns("Assortiment", "Plant")
	printdbcols(c)
	fmt.Println("\nRelationships for Assortiment.Plant:")
	fmt.Println(d.GetRelationships("Assortiment", "Plant"))
	fmt.Println("\nRelationships for Assortiment.Artikel:")
	fmt.Println(d.GetRelationships("Assortiment", "Artikel"))
	fmt.Println("\nRelationships for Assortiment.Voorraad:")
	fmt.Println(d.GetRelationships("Assortiment", "Voorraad"))
}

func testGraphql() {
	mx := http.NewServeMux()
	mx.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Test!"))
	})
	// d, err := connectTestPostgres()
	d, err := connectTestMysql()
	if err != nil {
		fmt.Println(err)
		return
	}

	schema, err := api.BuildSchema(api.BuildSchemaArgs{
		Tables: []string{},
		Conn:   d,
	})

	if err != nil {
		fmt.Println("Schema error:", err)
	}
	mx.HandleFunc("/graphql", func(w http.ResponseWriter, r *http.Request) {
		api.HandleGQL(&schema, w, r)
	})

	fmt.Println("Listening on port", listenAddr)
	log.Fatal(http.ListenAndServe(listenAddr, mx))

}

func printdbcols(cols []db.Column) {
	for _, c := range cols {
		fmt.Println("\nname:", c.Name)
		fmt.Println("type:", c.Type)
		fmt.Println("length:", c.Length)
		fmt.Println("nullable:", c.Nullable)
		fmt.Println("primary key:", c.PrimaryKey)
		fmt.Println("auto increment:", c.AutoIncrement)
		fmt.Println("default value:", c.DefaultValue)
		fmt.Println("value:", c.Value)
	}
}
func runAPIServer() {
	port := ":9999"
	mx := http.NewServeMux()
	// c, err := connectTestPostgres()
	c, err := connectTestMysql()
	if err != nil {
		log.Fatal(err)
	}
	mx.HandleFunc("/data/", api.RestHandler("/data", c))
	log.Println("Listening on port", port)
	log.Fatal(http.ListenAndServe(port, mx))
}

func testCreateTableSQL() {
	c, err := connectTestPostgres()
	// c, err := connectTestMysql()
	if err != nil {
		log.Fatal(err)
	}
	d, err := connectTestPostgres()
	// d, err := connectTestMysql()
	if err != nil {
		log.Fatal(err)
	}
	tbl, err := db.GetTable("assortiment", "artikel", c)
	if err != nil {
		log.Fatal(err)
	}
	sql, err := d.CreateTableSQL(&tbl)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(sql)
}

func testDb2Yml() {
	c, err := connectTestPostgres()
	// c, err := connectTestMysql()
	if err != nil {
		log.Fatal(err)
	}
	s, err := db.GetSchema("assortiment", c)
	if err != nil {
		log.Fatal(err)
	}
	b, err := db.Schema2Yaml(&s)
	// fmt.Println(string(b))
	err = ioutil.WriteFile("test.yml", b, 0770)
	if err != nil {
		log.Fatal(err)
	}
}

func testYml2Db() {
	yml, err := ioutil.ReadFile("test.yml")
	if err != nil {
		log.Fatal(err)
	}
	s, err := db.Yaml2Schema(yml)
	if err != nil {
		log.Fatal(err)
	}
	// fmt.Println(s)
	d, err := connectTestPostgres()
	// d, err := connectTestMysql()
	if err != nil {
		log.Fatal(err)
	}
	s.Name = s.Name + "1"
	fmt.Println(d.PreSQL())
	sql := d.DropSchemaSQL(s.Name)
	fmt.Println(sql)
	sql = d.CreateSchemaSQL(s.Name)
	fmt.Println(sql)
	for _, t := range s.Tables {
		t.Schema = t.Schema + "1"
		sql = d.DropTableSQL(&t)
		fmt.Println(sql)
		sql, err = d.CreateTableSQL(&t)
		fmt.Println(sql)
	}
	fmt.Println(d.PostSQL())
}
func testSort() {
	c, err := connectTestPostgres()
	// c, err := connectTestMysql()
	if err != nil {
		log.Fatal(err)
	}
	tbls := make([]db.Table, 0)
	rel, _ := db.GetTable("Relaties", "Relaties", c)
	plt, _ := db.GetTable("Assortiment", "Plant", c)
	or, _ := db.GetTable("Verkoop", "Orders", c)
	vor, _ := db.GetTable("Verkoop", "Orderregels", c)
	mt, _ := db.GetTable("Assortiment", "Maat", c)
	art, _ := db.GetTable("Assortiment", "Artikel", c)
	tbls = append(tbls, or, vor, art, mt, plt, rel)
	for _, t := range tbls {
		log.Println(t.Schema + "." + t.Name)
	}
	log.Println("---sorting---")
	db.SortTablesByForeignKey(tbls)
	for _, t := range tbls {
		log.Println(t.Schema + "." + t.Name)
	}
	// log.Println("---sorting---")
	// sort.Sort(db.TablesByKey(tbls))
	// for _, t := range tbls {
	// 	log.Println(t.Schema + "." + t.Name)
	// }
}
