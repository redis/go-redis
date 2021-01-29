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
		return m.(*structFields), ok
	}

	fMap := getStructFields(v, "redis")
	s.m.Store(t, fMap)
	return fmap, true
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

func getStructFields(ob reflect.Value, fieldTag string) *structFields {
	var (
		num = ob.NumField()
		out = newStructFields()
	)

	for i := 0; i < num; i++ {
		f := ob.Field(i)
		if !f.IsValid() || !f.CanSet() {
			continue
		}

		tag := ob.Type().Field(i).Tag.Get(fieldTag)
		if tag == "" || tag == "-" {
			continue
		}

		tag = strings.Split(tag, ",")[0]
		if tag == "" {
			continue
		}

		// Use the built-in decoder.
		out.set(tag, &structField{index: i, fn: decoders[f.Kind()]})
	}

	return out
}
