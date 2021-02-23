package libovsdb

import (
	"fmt"
)

// ORM offers functions to interact with libovsdb through user-provided native structs.
// The way to specify what field of the struct goes
// to what column in the database id through field a field tag.
// The tag used is "ovs" and has the following structure
// 'ovs:"${COLUMN_NAME}"'
//	where COLUMN_NAME is the name of the column and must match the schema
//
//Example:
//  type MyObj struct {
//  	Name string `ovs:"name"`
//  }
type orm struct {
	schema *DatabaseSchema
}

// ErrORM describes an error in an ORM type
type ErrORM struct {
	objType   string
	field     string
	fieldType string
	fieldTag  string
	reason    string
}

func (e *ErrORM) Error() string {
	return fmt.Sprintf("ORM Error. Object type %s contains field %s (%s) ovs tag %s: %s",
		e.objType, e.field, e.fieldType, e.fieldTag, e.reason)
}

// ErrNoTable describes a error in the provided table information
type ErrNoTable struct {
	table string
}

func (e *ErrNoTable) Error() string {
	return fmt.Sprintf("Table not found: %s", e.table)
}

// NewErrNoTable creates a new ErrNoTable
func NewErrNoTable(table string) error {
	return &ErrNoTable{
		table: table,
	}
}

// newORM returns a new ORM
func newORM(schema *DatabaseSchema) *orm {
	return &orm{
		schema: schema,
	}
}

// GetRowData transforms a Row to a struct based on its tags
// The result object must be given as pointer to an object with the right tags
func (o orm) getRowData(tableName string, row *Row, result interface{}) error {
	if row == nil {
		return nil
	}
	return o.getData(tableName, row.Fields, result)
}

// GetData transforms a map[string]interface{} containing OvS types (e.g: a ResultRow
// has this format) to orm struct
// The result object must be given as pointer to an object with the right tags
func (o orm) getData(tableName string, ovsData map[string]interface{}, result interface{}) error {
	table := o.schema.Table(tableName)
	if table == nil {
		return NewErrNoTable(tableName)
	}

	ormInfo, err := newORMInfo(table, result)
	if err != nil {
		return err
	}

	for name, column := range table.Columns {
		if !ormInfo.hasColumn(name) {
			// If provided struct does not have a field to hold this value, skip it
			continue
		}

		ovsElem, ok := ovsData[name]
		if !ok {
			// Ignore missing columns
			continue
		}

		nativeElem, err := OvsToNative(column, ovsElem)
		if err != nil {
			return fmt.Errorf("Table %s, Column %s: Failed to extract native element: %s",
				tableName, name, err.Error())
		}

		if err := ormInfo.setField(name, nativeElem); err != nil {
			return err
		}
	}
	return nil
}

// NewRow transforms an orm struct to a map[string] interface{} that can be used as libovsdb.Row
func (o orm) newRow(tableName string, data interface{}) (map[string]interface{}, error) {
	table := o.schema.Table(tableName)
	if table == nil {
		return nil, NewErrNoTable(tableName)
	}
	ormInfo, err := newORMInfo(table, data)
	if err != nil {
		return nil, err
	}
	ovsRow := make(map[string]interface{}, len(table.Columns))
	for name, column := range table.Columns {
		nativeElem, err := ormInfo.fieldByColumn(name)
		if err != nil {
			// If provided struct does not have a field to hold this value, skip it
			continue
		}

		if IsDefaultValue(column, nativeElem) {
			continue
		}
		ovsElem, err := NativeToOvs(column, nativeElem)
		if err != nil {
			return nil, fmt.Errorf("Table %s, Column %s: Failed to generate OvS element. %s", tableName, name, err.Error())
		}
		ovsRow[name] = ovsElem
	}
	return ovsRow, nil

}
