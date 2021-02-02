package hscan

import (
	"reflect"
	"strings"
	"sync"
)

// structField represents a single field in a target struct.
type structField struct {
	index int
	fn    decoderFunc
}

// structFields contains the list of all fields in a target struct.
type structFields struct {
	m map[string]*structField
}

// structMap contains the map of struct fields for target structs
// indexed by the struct type.
type structMap struct {
	m sync.Map
}

func newStructMap() *structMap {
	return new(structMap)
}

func (s *structMap) get(t reflect.Type) *structFields {
	if v, ok := s.m.Load(t); ok {
		return v.(*structFields)
	}

	fMap := getStructFields(t, "redis")
	s.m.Store(t, fMap)
	return fMap
}

func newStructFields() *structFields {
	return &structFields{
		m: make(map[string]*structField),
	}
}

func (s *structFields) set(tag string, sf *structField) {
	s.m[tag] = sf
}

func (s *structFields) get(tag string) (*structField, bool) {
	f, ok := s.m[tag]
	return f, ok
}

func getStructFields(t reflect.Type, fieldTag string) *structFields {
	var (
		num = t.NumField()
		out = newStructFields()
	)

	for i := 0; i < num; i++ {
		f := t.Field(i)

		tag := f.Tag.Get(fieldTag)
		if tag == "" || tag == "-" {
			continue
		}

		tag = strings.Split(tag, ",")[0]
		if tag == "" {
			continue
		}

		// Use the built-in decoder.
		out.set(tag, &structField{index: i, fn: decoders[f.Type.Kind()]})
	}

	return out
}
