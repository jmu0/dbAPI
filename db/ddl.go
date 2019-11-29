package db

//GetTable reads table struct from database
func GetTable(schemaName, tableName string, conn Conn) (Table, error) {
	var err error
	var rels []Relationship
	var fk ForeignKey
	var tbl = Table{
		Name:   tableName,
		Schema: schemaName,
	}
	tbl.Columns, err = conn.GetColumns(schemaName, tableName)
	if err != nil {
		return Table{}, err
	}
	rels, err = conn.GetRelationships(schemaName, tableName)
	for _, r := range rels {
		if r.Cardinality == "many-to-one" {
			fk = ForeignKey{
				Name:     r.Name,
				FromCols: r.FromCols,
				ToTable:  r.ToTable,
				ToCols:   r.ToCols,
			}
			tbl.ForeignKeys = append(tbl.ForeignKeys, fk)
		}
	}
	if err != nil {
		return Table{}, err
	}
	tbl.Indexes, err = conn.GetIndexes(schemaName, tableName)
	if err != nil {
		return Table{}, err
	}
	return tbl, nil
}

//GetSchema reads schema from database
func GetSchema(schemaName string, conn Conn) (Schema, error) {
	var tbls []string
	var err error
	var tbl Table

	var s = Schema{
		Name: schemaName,
	}
	tbls, err = conn.GetTableNames(schemaName)
	if err != nil {
		return Schema{}, err
	}
	for _, t := range tbls {
		tbl, err = GetTable(schemaName, t, conn)
		if err != nil {
			return Schema{}, err
		}
		s.Tables = append(s.Tables, tbl)
	}
	return s, nil
}

//UpdateTableSQL compares table struct to database and returns SQL to modify/create table in database
func UpdateTableSQL(tbl *Table, conn Conn, updateSchema bool) (string, error) {
	var sql, tmp string
	var err error
	if updateSchema && HasSchema(tbl.Schema, conn) == false {
		tmp := conn.CreateSchemaSQL(tbl.Schema)
		if len(sql) > 0 {
			sql += "\n"
		}
		sql += tmp
	}
	if HasTable(tbl.Schema, tbl.Name, conn) == false {
		tmp, err = conn.CreateTableSQL(tbl)
		if err != nil {
			return "", err
		}
		if len(sql) > 0 {
			sql += "\n"
		}
		sql += tmp
	} else {
		return "-- TODO: check table columns&foreign keys: " + tbl.Schema + "." + tbl.Name, nil
	}

	return sql, nil
}

//UpdateSchemaSQL compares schema struct to database and returns SQL to modify/create schema in database
func UpdateSchemaSQL(schema *Schema, conn Conn) (string, error) {
	var sql, tmp string
	var err error
	if HasSchema(schema.Name, conn) == false {
		tmp := conn.CreateSchemaSQL(schema.Name)
		if len(sql) > 0 {
			sql += "\n"
		}
		sql += tmp
	}
	// var printTables = func(tbls []Table) {
	// 	fmt.Println(">>>>>")
	// 	for _, t := range tbls {
	// 		fmt.Println(t.Name)
	// 	}
	// 	fmt.Println("<<<<<")
	// }
	// printTables(schema.Tables)
	SortTablesByForeignKey(schema.Tables)
	// printTables(schema.Tables)
	// log.Fatal("\nsorted!")
	for _, tbl := range schema.Tables {
		tmp, err = UpdateTableSQL(&tbl, conn, false)
		if err != nil {
			return "", err
		}
		if len(tmp) > 0 {
			sql += "\n" + tmp
		}
	}
	return sql, nil
}
