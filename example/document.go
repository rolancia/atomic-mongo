package example

import database "atomic-mongo"

type PersonDocument struct {
	database.Document

	*Person
}

func (rs *PersonDocument) Collection() string {
	return "person"
}

func (rs *PersonDocument) DocumentPtr() interface{} {
	return rs
}

func (rs *PersonDocument) DataPtr() interface{} {
	return rs.Person
}
