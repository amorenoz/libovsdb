package libovsdb

import (
	"encoding/json"
	"fmt"
	"io"
	"reflect"
)

// DatabaseSchema is a database schema according to RFC7047
type DatabaseSchema struct {
	Name    string                 `json:"name"`
	Version string                 `json:"version"`
	Tables  map[string]TableSchema `json:"tables"`
}

// TableSchema is a table schema according to RFC7047
type TableSchema struct {
	Columns map[string]*ColumnSchema `json:"columns"`
	Indexes [][]string               `json:"indexes,omitempty"`
}

/*RFC7047 defines some atomic-types (e.g: integer, string, etc). However, the Column's type
can also be other more complex types such as set, enum and map. The way to determine the type
depends on internal, not directly marshallable fields. Therefore, in order to simplify the usage
of this library, we define an ExtendedType that includes all possible column types (including
atomic fields).
*/

//ExtendedType includes atomic types as defined in the RFC plus Enum, Map and Set
type ExtendedType string

// RefType is used to define the possible RefTypes
type RefType string

const (
	//Unlimited is used to express unlimited "Max"
	Unlimited int = -1

	//Strong RefType
	Strong RefType = "strong"
	//Weak RefType
	Weak RefType = "weak"

	//ExtendedType associated with Atomic Types

	//TypeInteger is equivalent to 'int'
	TypeInteger ExtendedType = "integer"
	//TypeReal is equivalent to 'float64'
	TypeReal ExtendedType = "real"
	//TypeBoolean is equivalent to 'bool'
	TypeBoolean ExtendedType = "boolean"
	//TypeString is equivalent to 'string'
	TypeString ExtendedType = "string"
	//TypeUUID is equivalent to 'libovsdb.UUID'
	TypeUUID ExtendedType = "uuid"

	// Extended Types used to summarize the interal type of the field.

	//TypeEnum is an enumerator of type defined by Key.Type
	TypeEnum ExtendedType = "enum"
	//TypeMap is a map whose type depend on Key.Type and Value.Type
	TypeMap ExtendedType = "map"
	//TypeSet is a set whose type depend on Key.Type
	TypeSet ExtendedType = "set"
)

// ColumnSchema is a column schema according to RFC7047
type ColumnSchema struct {
	Name string `json:"name"`
	// According to RFC7047, "type" field can be, either an <atomic-type>
	// Or a ColumnTypeObject defined below. To try to simplify the usage, the
	// json message will be parsed manually and Type will indicate the "extended"
	// type. Depending on its value, more information may be available in TypeObj.
	// E.g: If Type == TypeEnum, TypeObj.Key.Enum contains the possible values
	Type      ExtendedType
	TypeObj   *ColumnTypeObject
	TypeMsg   json.RawMessage `json:"type"`
	Ephemeral bool            `json:"ephemeral,omitempty"`
	Mutable   bool            `json:"mutable,omitempty"`
}

// ColumnTypeObject is a type object as per RFC7047
type ColumnTypeObject struct {
	Key      *BaseType
	KeyMsg   *json.RawMessage `json:"key,omitempty"`
	Value    *BaseType
	ValueMsg *json.RawMessage `json:"value,omitempty"`
	Min      int              `json:"min,omitempty"`
	// Unlimited is expressed by the const value Unlimited (-1)
	Max    int
	MaxMsg *json.RawMessage `json:"max,omitempty"`
}

// BaseType is a base-type structure as per RFC7047
type BaseType struct {
	Type ExtendedType `json:"type"`
	// Enum will be parsed manually and set to a slice
	// of possible values. They must be type-asserted to the
	// corret type depending on the Type field
	Enum       OvsSet
	EnumMsg    *json.RawMessage `json:"enum,omitempty"`
	MinReal    float64          `json:"minReal,omitempty"`
	MaxReal    float64          `json:"maxReal,omitempty"`
	MinInteger int              `json:"minInteger,omitempty"`
	MaxInteger int              `json:"maxInteger,omitempty"`
	MinLength  int              `json:"minLength,omitempty"`
	MaxLength  int              `json:"maxLength,omitempty"`
	RefTable   string           `json:"refTable,omitempty"`
	RefType    RefType          `json:"refType,omitempty"`
}

// TypeString returns a string representation of the (native) column type
func (column *ColumnSchema) TypeString() string {
	switch column.Type {
	case TypeInteger, TypeReal, TypeBoolean, TypeString:
		return string(column.Type)
	case TypeUUID:
		return fmt.Sprintf("uuid [%s (%s)]", column.TypeObj.Key.RefTable, column.TypeObj.Key.RefType)
	case TypeEnum:
		return fmt.Sprintf("enum (type: %s): %v", column.TypeObj.Key.Type, column.TypeObj.Key.Enum)
	case TypeMap:
		return fmt.Sprintf("[%s]%s", column.TypeObj.Key.Type, column.TypeObj.Value.Type)
	case TypeSet:
		var keyStr string
		if column.TypeObj.Key.Type == TypeUUID {
			keyStr = fmt.Sprintf(" [%s (%s)]", column.TypeObj.Key.RefTable, column.TypeObj.Key.RefType)
		} else {
			keyStr = string(column.TypeObj.Key.Type)
		}
		return fmt.Sprintf("[]%s (min: %d, max: %d)", keyStr, column.TypeObj.Min, column.TypeObj.Max)
	default:
		return fmt.Sprintf("Unknown Type")
	}
}

//Unmarshal handles the manual unmarshalling of the ColumnSchema object
func (column *ColumnSchema) Unmarshal() error {
	// 'type' migth be a string or an object, figure it out
	if err := json.Unmarshal(column.TypeMsg, &column.Type); err == nil {
		return nil
	}

	column.TypeObj = &ColumnTypeObject{
		Key:   &BaseType{},
		Value: nil,
		Max:   1,
		Min:   1,
	}
	if err := json.Unmarshal(column.TypeMsg, column.TypeObj); err != nil {
		return err
	}

	// 'max' can be an integer or the string "unlimmited". To simplify, use "-1"
	// as unlimited
	if column.TypeObj.MaxMsg != nil {
		var maxString string
		if err := json.Unmarshal(*column.TypeObj.MaxMsg, &maxString); err == nil {
			if maxString == "unlimited" {
				column.TypeObj.Max = Unlimited
			} else {
				return fmt.Errorf("Unknown max value %s", maxString)
			}
		} else if err := json.Unmarshal(*column.TypeObj.MaxMsg, &column.TypeObj.Max); err != nil {
			return err
		}
	}

	// 'key' and 'value' can, themselves, be a string or a BaseType.
	// key='<atomic_type>' is equivalent to 'key': {'type': '<atomic_type>'}
	// To simplify things a bit, we'll translate the former to the latter
	if err := json.Unmarshal(*column.TypeObj.KeyMsg, &column.TypeObj.Key.Type); err != nil {
		if err := json.Unmarshal(*column.TypeObj.KeyMsg, column.TypeObj.Key); err != nil {
			return err

		}
	}
	if column.TypeObj.ValueMsg != nil {
		column.TypeObj.Value = &BaseType{}
		if err := json.Unmarshal(*column.TypeObj.ValueMsg, &column.TypeObj.Value.Type); err != nil {
			if err := json.Unmarshal(*column.TypeObj.ValueMsg, &column.TypeObj.Value); err != nil {
				return err

			}
		}
		column.Type = TypeMap
		return nil
	}

	// Try to parse key.Enum
	if column.TypeObj.Key.EnumMsg != nil {

		if err := column.TypeObj.Key.Enum.UnmarshalJSON(*column.TypeObj.Key.EnumMsg); err != nil {
			return nil
		}
		column.Type = TypeEnum
		return nil
	}

	if column.TypeObj.Min == 1 && column.TypeObj.Max == 1 {
		column.Type = column.TypeObj.Key.Type
	} else if column.TypeObj.Min != 1 || column.TypeObj.Max != 1 {
		column.Type = TypeSet
	}

	return nil
}

// Unmarshal parses the json and populates DatabaseSchema
func (schema *DatabaseSchema) Unmarshal(jsonBytes []byte) error {

	// 1. Unmarshal the outer structure
	if err := json.Unmarshal(jsonBytes, schema); err != nil {
		return err
	}

	// 2. Unmarshal column schemas
	for _, table := range schema.Tables {
		for _, column := range table.Columns {
			if err := column.Unmarshal(); err != nil {
				return err
			}
		}
	}
	return nil
}

// Print will print the contents of the DatabaseSchema
func (schema *DatabaseSchema) Print(w io.Writer) {
	fmt.Fprintf(w, "%s, (%s)\n", schema.Name, schema.Version)
	for table, tableSchema := range schema.Tables {
		fmt.Fprintf(w, "\t %s\n", table)
		for column, columnSchema := range tableSchema.Columns {
			fmt.Fprintf(w, "\t\t %s => %s\n", column, columnSchema.TypeString())
		}
	}
}

// Basic validation for operations against Database Schema
func (schema DatabaseSchema) validateOperations(operations ...Operation) bool {
	for _, op := range operations {
		table, ok := schema.Tables[op.Table]
		if ok {
			for column := range op.Row {
				if _, ok := table.Columns[column]; !ok {
					if column != "_uuid" && column != "_version" {
						return false
					}
				}
			}
			for _, row := range op.Rows {
				for column := range row {
					if _, ok := table.Columns[column]; !ok {
						if column != "_uuid" && column != "_version" {
							return false
						}
					}
				}
			}
			for _, column := range op.Columns {
				if _, ok := table.Columns[column]; !ok {
					if column != "_uuid" && column != "_version" {
						return false
					}
				}
			}
		} else {
			return false
		}
	}
	return true
}
