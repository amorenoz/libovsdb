package libovsdb

import (
	"fmt"
	"reflect"
)

// TableName is a a string representing a Table
type TableName = string

// A Model is the base interface used to build Database Models. It is used
// to express how data from a specific Database Table shall be translated into structs
// A Model is a struct with at least one (most likely more) field tagged with the 'ovs' tag
// The value of 'ovs' field must be a valid column name in the OVS Database
// A field named UUID is mandatory. The rest of the columns are optional
// The struct may also have non-tagged fields (which will be ignored by the API calls)
// The Model interface must be implemented by the pointer to such type
// Example:
//type MyLogicalRouter struct {
//	UUID          string            `ovs:"_uuid"`
//	Name          string            `ovs:"name"`
//	ExternalIDs   map[string]string `ovs:"external_ids"`
//	LoadBalancers []string          `ovs:"load_balancer"`
//}
//
//func (lr *MyLogicalRouter) Table() TableName {
//	return "Logical_Router"
//}
type Model interface {
	// Table returns the name of the Table this model represents
	Table() TableName
}

// DBModel is a Database model
type DBModel struct {
	name  string
	types map[TableName]reflect.Type
}

// newModel returns a new instance of a model from a specific TableName
func (db DBModel) newModel(table TableName) (Model, error) {
	mtype, ok := db.types[table]
	if !ok {
		return nil, fmt.Errorf("Table %s not found in Database Model", string(table))
	}
	model := reflect.New(mtype.Elem())
	return model.Interface().(Model), nil
}

// GetTypes returns the DBModel Types
// the DBModel types is a map of reflect.Types indexed by TableName
// The reflect.Type is a pointer to a struct that contains 'ovs' tags
// as described above. Such pointer to struct also implements the Model interface
func (db DBModel) Types() map[TableName]reflect.Type {
	return db.types
}

// Name returns the database name
func (db DBModel) Name() string {
	return db.name
}

// FindTable returns the TableName associated with a reflect.Type or ""
func (db DBModel) FindTable(mType reflect.Type) TableName {
	for table, tType := range db.types {
		if tType == mType {
			return table
		}
	}
	return ""
}

// NewDBModel constructs a DBModel based on a database name and slice of Models
func NewDBModel(name string, models []Model) (*DBModel, error) {
	types := make(map[TableName]reflect.Type, len(models))
	for _, model := range models {
		modelType := reflect.TypeOf(model)
		if modelType.Kind() != reflect.Ptr {
			return nil, fmt.Errorf("Model is expected to be a pointer to struct")
		}
		if modelType.Elem().Kind() != reflect.Struct {
			return nil, fmt.Errorf("Model is expected to be a pointer to struct")
		}
		hasUUID := false
		for i := 0; i < modelType.Elem().NumField(); i++ {
			if field := modelType.Elem().Field(i); field.Tag.Get("ovs") == "_uuid" &&
				field.Type.Kind() == reflect.String {
				hasUUID = true
			}
		}
		if !hasUUID {
			return nil, fmt.Errorf("Model is expected to have a string field called UUID")
		}

		types[model.Table()] = reflect.TypeOf(model)
	}
	return &DBModel{
		types: types,
		name:  name,
	}, nil
}

func modelSetUUID(model Model, uuid string) error {
	modelVal := reflect.ValueOf(model).Elem()
	for i := 0; i < modelVal.NumField(); i++ {
		if field := modelVal.Type().Field(i); field.Tag.Get("ovs") == "_uuid" &&
			field.Type.Kind() == reflect.String {
			modelVal.Field(i).Set(reflect.ValueOf(uuid))
			return nil
		}
	}
	return fmt.Errorf("Model is expected to have a string field mapped to column _uuid")
}
