package libovsdb

import (
	"encoding/json"
)

// OvsRow is a table Row according to RFC7047
// It contains OVS-specific format
type OvsRow struct {
	Fields map[string]interface{}
}

// Row is a table Row without OvS-specific fields
// go slices instead of OvsSets
// go strings instead of UUIDs
// go maps instead of OvsMaps
type Row map[string]interface{}

// UnmarshalJSON unmarshalls a byte array to an OVSDB Row
func (r *OvsRow) UnmarshalJSON(b []byte) (err error) {
	r.Fields = make(map[string]interface{})
	var raw map[string]interface{}
	err = json.Unmarshal(b, &raw)
	for key, val := range raw {
		val, err = ovsSliceToGoNotation(val)
		if err != nil {
			return err
		}
		r.Fields[key] = val
	}
	return err
}

// Data returns the data in native format
// This is, removing OvsSet, UUID, and OvsMap from the way
func (r *OvsResultRow) Data(table *TableSchema) (*Row, error) {
	return ovsRowToNative(table, *r)
}

// OvsResultRow is an properly unmarshalled row returned by Transact
type OvsResultRow map[string]interface{}

// UnmarshalJSON unmarshalls a byte array to an OVSDB Row
func (r *OvsResultRow) UnmarshalJSON(b []byte) (err error) {
	*r = make(map[string]interface{})
	var raw map[string]interface{}
	err = json.Unmarshal(b, &raw)
	for key, val := range raw {
		val, err = ovsSliceToGoNotation(val)
		if err != nil {
			return err
		}
		(*r)[key] = val
	}
	return err
}

// Data returns the data in native format
// This is, removing OvsSet, UUID, and OvsMap from the way
func (r *OvsRow) Data(table *TableSchema) (*Row, error) {
	return ovsRowToNative(table, r.Fields)
}

// NewOvsRow returns a OvsRow from a map of native types
// Type validation is performed against the table schema provided
func NewOvsRow(native map[string]interface{}, table *TableSchema) (*OvsRow, error) {
	row := make(map[string]interface{})
	for name, column := range table.Columns {
		rawElem, ok := native[name]
		if !ok {
			// It's OK if the native struct has less collumns than the Table
			continue
		}
		ovsElem, err := column.NativeToOvs(rawElem)
		if err != nil {
			return nil, err
		}
		row[name] = ovsElem
	}
	return &OvsRow{row}, nil
}

func ovsRowToNative(table *TableSchema, ovsRow map[string]interface{}) (*Row, error) {
	row := Row{}
	for name, column := range table.Columns {
		ovsElem, ok := ovsRow[name]
		if !ok {
			continue
		}
		nativeElem, err := column.OvsToNative(ovsElem)
		if err != nil {
			return nil, err
		}
		row[name] = nativeElem
	}
	return &row, nil
}
