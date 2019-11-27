package db

import (
	"gopkg.in/yaml.v2"
)

//Schema2Yaml creates yml string from schema
func Schema2Yaml(s *Schema) ([]byte, error) {
	var err error
	var bytes []byte
	bytes, err = yaml.Marshal(s)
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
