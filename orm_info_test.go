package libovsdb

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

var sampleTable = []byte(`{
      "columns": {
        "aString": {
          "type": "string"
        },
        "aInteger": {
          "type": "integer"
        },
        "aSet": {
          "type": {
            "key": "string",
            "max": "unlimited",
            "min": 0
          }
        },
        "aMap": {
          "type": {
            "key": "string",
            "value": "string"
          }
        }
    }
}`)

func TestNewOrmInfo(t *testing.T) {
	type test struct {
		name         string
		table        []byte
		obj          interface{}
		expectedCols []string
		err          bool
	}
	tests := []test{
		{
			name:  "no_orm",
			table: sampleTable,
			obj: &struct {
				foo string
				bar int
			}{},
			err: false,
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("NewOrm_%s", tt.name), func(t *testing.T) {
			var table TableSchema
			err := json.Unmarshal(tt.table, &table)
			assert.Nil(t, err)

			info, err := newORMInfo(&table, tt.obj)
			if tt.err {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
			}
			for _, col := range tt.expectedCols {
				assert.Truef(t, info.hasColumn(col), "Expected column should be present in ORM Info")
			}

		})
	}
}

func TestOrmInfoSet(t *testing.T) {
	type obj struct {
		Ostring string            `ovs:"aString"`
		Oint    int               `ovs:"aInteger"`
		Oset    []string          `ovs:"aSet"`
		Omap    map[string]string `ovs:"aMap"`
	}

	type test struct {
		name   string
		table  []byte
		obj    interface{}
		field  interface{}
		column string
		err    bool
	}
	tests := []test{
		{
			name:   "string",
			table:  sampleTable,
			obj:    &obj{},
			field:  "foo",
			column: "aString",
			err:    false,
		},
		{
			name:   "set",
			table:  sampleTable,
			obj:    &obj{},
			field:  []string{"foo", "bar"},
			column: "aSet",
			err:    false,
		},
		{
			name:   "map",
			table:  sampleTable,
			obj:    &obj{},
			field:  map[string]string{"foo": "bar"},
			column: "aMap",
			err:    false,
		},
		{
			name:  "nonempty",
			table: sampleTable,
			obj: &obj{
				Omap:    map[string]string{"original": "stuff"},
				Oint:    1,
				Ostring: "foo",
				Oset:    []string{"foo"},
			},
			field:  map[string]string{"foo": "bar"},
			column: "aMap",
			err:    false,
		},
		{
			name:   "unassignalbe",
			table:  sampleTable,
			obj:    &obj{},
			field:  []string{"foo"},
			column: "aMap",
			err:    true,
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("SetField_%s", tt.name), func(t *testing.T) {
			var table TableSchema
			err := json.Unmarshal(tt.table, &table)
			assert.Nil(t, err)

			info, err := newORMInfo(&table, tt.obj)
			assert.Nil(t, err)

			err = info.setField(tt.column, tt.field)
			if tt.err {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
				readBack, err := info.fieldByColumn(tt.column)
				assert.Nil(t, err)
				assert.Equalf(t, tt.field, readBack, "Set field should match original")
			}

		})
	}
}

func TestOrmInfoColByPtr(t *testing.T) {
	type obj struct {
		ostring string            `ovs:"aString"`
		oint    int               `ovs:"aInteger"`
		oset    []string          `ovs:"aSet"`
		omap    map[string]string `ovs:"aMap"`
	}
	obj1 := obj{}

	type test struct {
		name   string
		table  []byte
		obj    interface{}
		field  interface{}
		column string
		err    bool
	}
	tests := []test{
		{
			name:   "first",
			table:  sampleTable,
			obj:    &obj1,
			field:  &obj1.ostring,
			column: "aString",
			err:    false,
		},
		{
			name:   "middle",
			table:  sampleTable,
			obj:    &obj1,
			field:  &obj1.oint,
			column: "aInteger",
			err:    false,
		},
		{
			name:   "middle2",
			table:  sampleTable,
			obj:    &obj1,
			field:  &obj1.oset,
			column: "aSet",
			err:    false,
		},
		{
			name:   "last",
			table:  sampleTable,
			obj:    &obj1,
			field:  &obj1.omap,
			column: "aMap",
			err:    false,
		},
		{
			name:  "external",
			table: sampleTable,
			obj:   &obj1,
			field: &obj{},
			err:   true,
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("GetFieldByPtr_%s", tt.name), func(t *testing.T) {
			var table TableSchema
			err := json.Unmarshal(tt.table, &table)
			assert.Nil(t, err)

			info, err := newORMInfo(&table, tt.obj)
			assert.Nil(t, err)

			col, err := info.columnByPtr(tt.field)
			if tt.err {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
				assert.Equalf(t, tt.column, col, "Column name extracted should match")
			}

		})
	}
}

func TestOrmGetIndex(t *testing.T) {
	tableSchema := []byte(`{
      "indexes": [["name"],["composed_1","composed_2"]],
      "columns": {
        "name": {
          "type": "string"
        },
        "composed_1": {
          "type": {
            "key": "string"
          }
        },
        "composed_2": {
          "type": {
            "key": "string"
          }
        },
        "config": {
          "type": {
            "key": "string",
            "max": "unlimited",
            "min": 0,
            "value": "string"
          }
	}
      }
   }`)
	var table TableSchema
	err := json.Unmarshal(tableSchema, &table)
	assert.Nil(t, err)

	type obj struct {
		ID     string            `ovs:"_uuid"`
		MyName string            `ovs:"name"`
		Config map[string]string `ovs:"config"`
		Comp1  string            `ovs:"composed_1"`
		Comp2  string            `ovs:"composed_2"`
	}
	type test struct {
		name     string
		obj      interface{}
		expected [][]string
		err      bool
	}
	tests := []test{
		{
			name:     "empty",
			obj:      &obj{},
			expected: [][]string{},
			err:      false,
		},
		{
			name: "UUID",
			obj: &obj{
				ID: aUUID0,
			},
			expected: [][]string{{"_uuid"}},
			err:      false,
		},
		{
			name: "simple",
			obj: &obj{
				MyName: "foo",
			},
			expected: [][]string{{"name"}},
			err:      false,
		},
		{
			name: "additional index",
			obj: &obj{
				ID:     aUUID0,
				MyName: "foo",
			},
			expected: [][]string{{"_uuid"}, {"name"}},
			err:      false,
		},
		{
			name: "complex index",
			obj: &obj{
				Comp1: "foo",
				Comp2: "bar",
			},
			expected: [][]string{{"composed_1", "composed_2"}},
			err:      false,
		},
		{
			name: "multiple index",
			obj: &obj{
				MyName: "something",
				Comp1:  "foo",
				Comp2:  "bar",
			},
			expected: [][]string{{"name"}, {"composed_1", "composed_2"}},
			err:      false,
		},
		{
			name: "all ",
			obj: &obj{
				ID:     aUUID0,
				MyName: "something",
				Comp1:  "foo",
				Comp2:  "bar",
			},
			expected: [][]string{{"_uuid"}, {"name"}, {"composed_1", "composed_2"}},
			err:      false,
		},
		{
			name: "Error: None",
			obj: &obj{
				Config: map[string]string{"foo": "bar"},
			},
			expected: [][]string{},
			err:      false,
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("GetValidIndexes_%s", tt.name), func(t *testing.T) {
			info, err := newORMInfo(&table, tt.obj)
			assert.Nil(t, err)

			indexes, err := info.getValidORMIndexes()
			if tt.err {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
				assert.ElementsMatchf(t, tt.expected, indexes, "Indexes must match")
			}

		})
	}
}
