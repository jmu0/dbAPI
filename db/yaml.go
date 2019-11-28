package db

import (
	"gopkg.in/yaml.v2"
)

//Schema2Yaml creates yml string from schema struct
func Schema2Yaml(s *Schema) ([]byte, error) {
	var err error
	var bytes []byte
	bytes, err = yaml.Marshal(s)
	if err != nil {
		return []byte(""), err
	}
	return bytes, nil
}

//Table2Yaml creates yml string from table struct
func Table2Yaml(t *Table) ([]byte, error) {
	var err error
	var bytes []byte
	bytes, err = yaml.Marshal(t)
	if err != nil {
		return []byte(""), err
	}
	return bytes, nil
}

//Yaml2Schema parses yml string to schema struct
func Yaml2Schema(yml []byte) (Schema, error) {
	var s = Schema{}
	var err error
	err = yaml.Unmarshal(yml, &s)
	if err != nil {
		return Schema{}, err
	}
	return s, nil
}

//Yaml2Table parses yml string to table struct
func Yaml2Table(yml []byte) (Table, error) {
	var t = Table{}
	var err error
	err = yaml.Unmarshal(yml, &t)
	if err != nil {
		return Table{}, err
	}
	return t, nil
}
