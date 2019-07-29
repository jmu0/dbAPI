package mysql

/*
//ToMap database object to map
func ToMap(obj DbObject) map[string]interface{} {
	cols := obj.GetColumns()
	m := make(map[string]interface{})
	for _, col := range cols {
		m[col.Field] = col.Value
	}
	return m
}

//ToMapSlice database objects to slice of maps
func ToMapSlice(slice []DbObject) []map[string]interface{} {
	ret := make([]map[string]interface{}, 0)
	for _, obj := range slice {
		ret = append(ret, ToMap(obj))
	}
	return ret
}

//Save database object using statement
func Save(obj DbObject) (int, error) {
	dbName, tblName := obj.GetDbInfo()
	cols := obj.GetColumns()
	n, _, err := save(dbName, tblName, cols)
	return n, err
}


/*
//SaveQuery (DEPRECATED) Save database object to database (insert or update) using insert query
func SaveQuery(obj DbObject) (int, error) {
	dbName, tblName := obj.GetDbInfo()
	cols := obj.GetColumns()
	db, err := Connect()
	if err != nil {
		return 1, err
	}
	defer db.Close()
	query := "insert into " + dbName + "." + tblName + " "
	fields := "("
	values := "("
	update := ""
	for i, c := range cols {
		if len(fields) > 1 && i < len(cols) {
			fields += ", "
			values += ", "
			update += ", "
		}
		fields += c.Field
		values += valueString(c.Value)
		update += c.Field + "=" + valueString(c.Value)

	}
	fields += ") "
	values += ") "
	query += fields + " values " + values
	query += " on duplicate key update " + update
	_, err = db.Exec(query)
	if err != nil {
		return 1, err
	}
	return 0, nil
}

//Delete database object from database
func Delete(obj DbObject) (int, error) {
	dbName, tblName := obj.GetDbInfo()
	cols := obj.GetColumns()
	return delete(dbName, tblName, cols)
}

//SaveQuery (DEPRECATED) Save database object to database (insert or update) using insert query
func SaveQuery(obj DbObject) (int, error) {
	dbName, tblName := obj.GetDbInfo()
	cols := obj.GetColumns()
	db, err := Connect()
	if err != nil {
		return 1, err
	}
	defer db.Close()
	query := "insert into " + dbName + "." + tblName + " "
	fields := "("
	values := "("
	update := ""
	for i, c := range cols {
		if len(fields) > 1 && i < len(cols) {
			fields += ", "
			values += ", "
			update += ", "
		}
		fields += c.Field
		values += valueString(c.Value)
		update += c.Field + "=" + valueString(c.Value)

	}
	fields += ") "
	values += ") "
	query += fields + " values " + values
	query += " on duplicate key update " + update
	_, err = db.Exec(query)
	if err != nil {
		return 1, err
	}
	return 0, nil
}

//Delete database object from database
func Delete(obj DbObject) (int, error) {
	dbName, tblName := obj.GetDbInfo()
	cols := obj.GetColumns()
	return delete(dbName, tblName, cols)
}

//find out if the class has int columns, then it neets strconv import
func hasIntColumns(cols []Column) bool {
	for _, c := range cols {
		if GetType(c.Type) == "int" {
			return true
		}
	}
	return false
}

//CreateObject create object from db/table
func CreateObject(db *sql.DB, dbName, tblName string) error {
	var code string
	var importPrefix = "github.com/jmu0/orm/"
	cols := GetColumns(db, dbName, tblName)

	code += "package " + strings.ToLower(tblName) + "\n\n"
	code += "import (\n\t\"" + importPrefix + "dbmodel\"\n"
	code += "\t\"errors\"\n"
	if hasIntColumns(cols) {
		code += "\t\"strconv\"\n"
	}
	code += ")\n\n"
	code += "type " + tblName + " struct {\n"
	for _, col := range cols {
		code += "\t" + strings.ToUpper(col.Field[:1]) + col.Field[1:] + " " + GetType(col.Type) + "\n"
	}
	code += "}\n\n"
	code += "func (" + strings.ToLower(tblName[:1]) + " *" + tblName + ") GetDbInfo() (dbName string, tblName string) {\n"
	code += "\treturn \"" + dbName + "\", \"" + tblName + "\"\n"
	code += "}\n\n"
	code += strGetQueryFunction(cols, dbName, tblName)
	code += strGetSaveFunction(cols, dbName, tblName)
	code += strGetDeleteFunction(cols, dbName, tblName)
	code += strGetGetFunction(cols, tblName)
	code += strGetSetFunction(cols, tblName)
	code += strGetColsFunction(cols, tblName)

	//Write to file
	folder := strings.ToLower(dbName) + "/" + strings.ToLower(tblName)
	err := os.MkdirAll(folder, 0770)
	if err != nil {
		log.Fatal(err)
	}
	path := folder + "/" + strings.ToLower(tblName) + ".go"
	file, err := os.Create(path)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	_, err = file.WriteString(code)
	if err != nil {
		log.Fatal(err)
	}
	return nil
}

func strGetColsFunction(c []Column, tblName string) string {
	var ret string
	ret = "func (" + strings.ToLower(tblName[:1]) + " *" + tblName + ") GetColumns() []dbmodel.Column {\n"
	ret += "\treturn []dbmodel.Column{\n"
	for _, col := range c {
		ret += "\t\t{\n"
		ret += "\t\t\tField:\"" + col.Field + "\",\n"
		ret += "\t\t\tType:\"" + col.Type + "\",\n"
		ret += "\t\t\tNull:\"" + col.Null + "\",\n"
		ret += "\t\t\tKey:\"" + col.Key + "\",\n"
		ret += "\t\t\tDefault:\"" + col.Default + "\",\n"
		ret += "\t\t\tExtra:\"" + col.Extra + "\",\n"
		ret += "\t\t\tValue: " + strings.ToLower(tblName)[:1] + "."
		ret += strings.ToUpper(col.Field[:1]) + col.Field[1:] + ",\n"
		ret += "\t\t},\n"
	}
	ret += "\t}\n"
	ret += "}\n\n"
	return ret
}

func strGetSetFunction(c []Column, tblName string) string {
	var ret string
	var letter = strings.ToLower(tblName[:1])
	ret = "func (" + letter + " *" + tblName + ") Set(key string, value interface{}) error {\n"
	if hasIntColumns(c) {
		ret += "\tvar err error\n"
	}
	ret += "\tif  value == nil {\n"
	ret += "\t\treturn errors.New(\"value for \" + key + \" is nil\")\n"
	ret += "\t}\n"
	ret += "\tswitch key {\n"
	for _, col := range c {
		ret += "\tcase \"" + col.Field + "\":\n" //TODO: capitalize fields
		if GetType(col.Type) == "int" {
			ret += "\t\t" + letter + "."
			ret += strings.ToUpper(col.Field[:1]) + col.Field[1:]
			ret += ", err = strconv.Atoi(value.(string))\n"
			ret += "\t\tif err != nil && value != \"NULL\" {\n"
			ret += "\t\t\treturn err\n"
			ret += "\t\t}\n"
		} else {
			ret += "\t\t" + letter + "." + strings.ToUpper(col.Field[:1]) + col.Field[1:] + " = value.(string)\n"
		}
		ret += "\t\treturn nil\n"
	}
	ret += "\tdefault:\n"
	ret += "\t\treturn errors.New(\"Key not found:\" + key)\n"
	ret += "\t}\n"
	ret += "}\n\n"
	return ret
}

func strGetGetFunction(c []Column, tblName string) string {
	var ret string
	var letter = strings.ToLower(tblName[:1])
	ret = "func (" + letter + " *" + tblName + ") Get(key string) (dbmodel.Column, error) {\n"
	ret += "\tfor _, col := range " + letter + ".GetColumns() {\n"
	ret += "\t\tif col.Field == key {\n"
	ret += "\t\t\treturn col, nil\n"
	ret += "\t\t}\n"
	ret += "\t}\n"
	ret += "\treturn dbmodel.Column{}, errors.New(\"Key not found:\" + key)\n"
	ret += "}\n\n"
	return ret
}

func strGetSaveFunction(c []Column, dbName string, tblName string) string {
	var ret string
	ret = "func (" + strings.ToLower(tblName[:1]) + " *" + tblName + ") Save() (Nr int, err error) {\n"
	ret += "\treturn dbmodel.Save(" + strings.ToLower(tblName[:1]) + ")\n"
	ret += "}\n\n"
	return ret
}

func strGetDeleteFunction(c []Column, dbName string, tblName string) string {
	var ret string
	ret = "func (" + strings.ToLower(tblName[:1]) + " *" + tblName + ") Delete() (Nr int, err error) {\n"
	ret += "\treturn dbmodel.Delete(" + strings.ToLower(tblName[:1]) + ")\n"
	ret += "}\n\n"
	return ret
}

func strGetQueryFunction(cols []Column, dbName string, tblName string) string {
	//TODO: with this code integer fields cannot be null. change to check for ""
	var ret string
	ret = "func Query(where string, orderby string) ([]" + tblName + ", error) {\n"
	ret += "\tquery := \"select * from " + dbName + "." + tblName + "\"\n"
	ret += "\tif len(where) > 0 {\n\t\tquery += \" where \" + where\n\t}\n"
	ret += "\tif len(orderby) > 0 {\n\t\tquery += \" order by \" + orderby\n\t}\n"
	ret += "\tret := []" + tblName + "{}\n"
	ret += "\tdb, err := dbmodel.Connect()\n"
	ret += "\tdefer db.Close()\n"
	ret += "\tif err != nil {\n\t\treturn ret, err\n\t}\n"
	ret += "\tres,err := dbmodel.Query(db, query)\n"
	ret += "\tif err != nil {\n\t\treturn ret, err\n\t}\n"
	ret += "\tfor _, r := range res {\n"
	if hasIntColumns(cols) {
		ret += "\t\tvar err error\n"
	}
	ret += "\t\tobj := " + tblName + "{}\n"
	for _, c := range cols {
		tp := GetType(c.Type)
		if tp == "int" {
			// ret += "\t\tobj." + strings.ToUpper(c.Field[:1]) + c.Field[1:] + " = "
			// ret += "r[\"" + c.Field + "\"].(int)\n"
			ret += "\t\tobj." + strings.ToUpper(c.Field[:1]) + c.Field[1:] + ", err = "
			ret += "strconv.Atoi(r[\"" + c.Field + "\"].(string))\n"
			ret += "\t\tif err != nil && r[\"" + c.Field + "\"] != \"NULL\" && r[\"" + c.Field + "\"] != \"\""
			ret += " {\n\t\t\treturn ret, err\n\t\t}\n"
		} else {
			ret += "\t\tif r[\"" + c.Field + "\"] != nil {\n"
			ret += "\t\t\tobj." + strings.ToUpper(c.Field[:1]) + c.Field[1:] + " = "
			ret += "r[\"" + c.Field + "\"].(string)\n"
			ret += "\t\t}\n"
		}
	}
	ret += "\t\tret = append(ret, obj)\n"
	ret += "\t}\n"
	ret += "\tif len(ret) == 0 {\n\t\treturn ret, errors.New(\"No rows found\")\n\t}\n"
	ret += "\treturn ret, nil\n"
	ret += "}\n\n"
	return ret
}

//*/
