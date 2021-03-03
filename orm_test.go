package libovsdb

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

var testSchema = []byte(`{
  "cksum": "223619766 22548",
  "name": "TestSchema",
  "tables": {
    "TestTable": {
      "columns": {
        "aString": {
          "type": "string"
        },
        "aSet": {
          "type": {
            "key": "string",
            "max": "unlimited",
            "min": 0
          }
        },
        "aSingleSet": {
          "type": {
            "key": "string",
            "max": "unlimited",
            "min": 0,
            "max": 1
          }
        },
        "aUUIDSet": {
          "type": {
            "key": {
              "refTable": "SomeOtherTAble",
              "refType": "weak",
              "type": "uuid"
            },
            "min": 0
          }
        },
        "aUUID": {
          "type": {
            "key": {
              "refTable": "SomeOtherTAble",
              "refType": "weak",
              "type": "uuid"
            },
            "min": 1,
            "max": 1
          }
        },
        "aIntSet": {
          "type": {
            "key": {
              "type": "integer"
            },
            "min": 0,
            "max": "unlimited"
          }
        },
        "aFloat": {
          "type": {
            "key": {
              "type": "real"
            }
          }
        },
        "aFloatSet": {
          "type": {
            "key": {
              "type": "real"
            },
            "min": 0,
            "max": 10
          }
        },
        "aEmptySet": {
          "type": {
            "key": {
              "type": "string"
            },
            "min": 0,
            "max": "unlimited"
          }
        },
        "aEnum": {
          "type": {
            "key": {
              "enum": [
                "set",
                [
                  "enum1",
                  "enum2",
                  "enum3"
                ]
              ],
              "type": "string"
            }
          }
        },
        "aMap": {
          "type": {
            "key": "string",
            "max": "unlimited",
            "min": 0,
            "value": "string"
          }
	}
      }
    }
  }
}`)

func getOvsTestRow(t *testing.T) Row {
	ovsRow := Row{Fields: make(map[string]interface{})}
	ovsRow.Fields["aString"] = aString
	ovsRow.Fields["aSet"] = *testOvsSet(t, aSet)
	// Set's can hold the value if they have len == 1
	ovsRow.Fields["aSingleSet"] = aString

	us := make([]UUID, 0)
	for _, u := range aUUIDSet {
		us = append(us, UUID{GoUUID: u})
	}
	ovsRow.Fields["aUUIDSet"] = *testOvsSet(t, us)

	ovsRow.Fields["aUUID"] = UUID{GoUUID: aUUID0}

	ovsRow.Fields["aIntSet"] = *testOvsSet(t, aIntSet)

	ovsRow.Fields["aFloat"] = aFloat

	ovsRow.Fields["aFloatSet"] = *testOvsSet(t, aFloatSet)

	ovsRow.Fields["aEmptySet"] = *testOvsSet(t, []string{})

	ovsRow.Fields["aEnum"] = aEnum

	ovsRow.Fields["aMap"] = *testOvsMap(t, aMap)

	return ovsRow
}

func TestORMGetData(t *testing.T) {
	type ormTestType struct {
		AString             string            `ovs:"aString"`
		ASet                []string          `ovs:"aSet"`
		ASingleSet          []string          `ovs:"aSingleSet"`
		AUUIDSet            []string          `ovs:"aUUIDSet"`
		AUUID               string            `ovs:"aUUID"`
		AIntSet             []int             `ovs:"aIntSet"`
		AFloat              float64           `ovs:"aFloat"`
		AFloatSet           []float64         `ovs:"aFloatSet"`
		YetAnotherStringSet []string          `ovs:"aEmptySet"`
		AEnum               string            `ovs:"aEnum"`
		AMap                map[string]string `ovs:"aMap"`
		NonTagged           string
	}

	var expected = ormTestType{
		AString:             aString,
		ASet:                aSet,
		ASingleSet:          []string{aString},
		AUUIDSet:            aUUIDSet,
		AUUID:               aUUID0,
		AIntSet:             aIntSet,
		AFloat:              aFloat,
		AFloatSet:           aFloatSet,
		YetAnotherStringSet: []string{},
		AEnum:               aEnum,
		AMap:                aMap,
		NonTagged:           "something",
	}

	ovsRow := getOvsTestRow(t)
	/* Code under test */
	var schema DatabaseSchema
	if err := json.Unmarshal(testSchema, &schema); err != nil {
		t.Error(err)
	}

	orm := newORM(&schema)
	test := ormTestType{
		NonTagged: "something",
	}
	err := orm.getRowData("TestTable", &ovsRow, &test)
	/*End code under test*/

	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, expected, test)
}

func TestORMNewRow(t *testing.T) {
	var schema DatabaseSchema
	if err := json.Unmarshal(testSchema, &schema); err != nil {
		t.Error(err)
	}

	tests := []struct {
		name        string
		objInput    interface{}
		expectedRow map[string]interface{}
	}{{
		name: "string",
		objInput: &struct {
			AString string `ovs:"aString"`
		}{
			AString: aString,
		},
		expectedRow: map[string]interface{}{"aString": aString},
	}, {
		name: "set",
		objInput: &struct {
			SomeSet []string `ovs:"aSet"`
		}{
			SomeSet: aSet,
		},
		expectedRow: map[string]interface{}{"aSet": testOvsSet(t, aSet)},
	}, {
		name: "emptySet",
		objInput: &struct {
			EmptySet []string `ovs:"aSet"`
		}{
			EmptySet: []string{},
		},
		expectedRow: map[string]interface{}{}, // Default values don't make it into the row
	}, {
		name: "UUID",
		objInput: &struct {
			MyUUID string `ovs:"aUUID"`
		}{
			MyUUID: aUUID0,
		},
		expectedRow: map[string]interface{}{"aUUID": UUID{GoUUID: aUUID0}},
	}, {
		name: "aUUIDSet",
		objInput: &struct {
			MyUUIDSet []string `ovs:"aUUIDSet"`
		}{
			MyUUIDSet: []string{aUUID0, aUUID1},
		},
		expectedRow: map[string]interface{}{"aUUIDSet": testOvsSet(t, []UUID{{GoUUID: aUUID0}, {GoUUID: aUUID1}})},
	}, {
		name: "aIntSet",
		objInput: &struct {
			MyIntSet []int `ovs:"aIntSet"`
		}{
			MyIntSet: []int{0, 42},
		},
		expectedRow: map[string]interface{}{"aIntSet": testOvsSet(t, []int{0, 42})},
	}, {
		name: "aFloat",
		objInput: &struct {
			MyFloat float64 `ovs:"aFloat"`
		}{
			MyFloat: 42.42,
		},
		expectedRow: map[string]interface{}{"aFloat": 42.42},
	}, {
		name: "aFloatSet",
		objInput: &struct {
			MyFloatSet []float64 `ovs:"aFloatSet"`
		}{
			MyFloatSet: aFloatSet,
		},
		expectedRow: map[string]interface{}{"aFloatSet": testOvsSet(t, aFloatSet)},
	}, {
		name: "Enum",
		objInput: &struct {
			MyEnum string `ovs:"aEnum"`
		}{
			MyEnum: aEnum,
		},
		expectedRow: map[string]interface{}{"aEnum": aEnum},
	}, {
		name: "untagged fields should not affect row",
		objInput: &struct {
			AString string `ovs:"aString"`
			MyStuff map[string]string
		}{
			AString: aString,
			MyStuff: map[string]string{"this is": "private"},
		},
		expectedRow: map[string]interface{}{"aString": aString},
	}, {
		name: "Maps",
		objInput: &struct {
			MyMap map[string]string `ovs:"aMap"`
		}{
			MyMap: aMap,
		},
		expectedRow: map[string]interface{}{"aMap": testOvsMap(t, aMap)},
	},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("NewRow: %s", test.name), func(t *testing.T) {
			orm := newORM(&schema)
			row, err := orm.newRow("TestTable", test.objInput)
			assert.Nil(t, err)
			assert.Equalf(t, test.expectedRow, row, "NewRow should match expeted")
		})
	}
}

func TestORMCondition(t *testing.T) {

	var testSchema = []byte(`{
  "cksum": "223619766 22548",
  "name": "TestSchema",
  "tables": {
    "TestTable": {
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
    }
  }
}`)
	type testType struct {
		ID     string            `ovs:"_uuid"`
		MyName string            `ovs:"name"`
		Config map[string]string `ovs:"config"`
		Comp1  string            `ovs:"composed_1"`
		Comp2  string            `ovs:"composed_2"`
	}

	var schema DatabaseSchema
	if err := json.Unmarshal(testSchema, &schema); err != nil {
		t.Fatal(err)
	}
	orm := newORM(&schema)

	type Test struct {
		name     string
		prepare  func(*testType)
		expected []interface{}
		index    []interface{}
		err      bool
	}
	testObj := testType{}

	tests := []Test{
		{
			name: "simple index",
			prepare: func(t *testType) {
				t.ID = ""
				t.MyName = "foo"
				t.Config = nil
				t.Comp1 = ""
				t.Comp2 = ""
			},
			index:    []interface{}{},
			expected: []interface{}{[]interface{}{"name", "==", "foo"}},
			err:      false,
		},
		{
			name: "UUID",
			prepare: func(t *testType) {
				t.ID = aUUID0
				t.MyName = "foo"
				t.Config = nil
				t.Comp1 = ""
				t.Comp2 = ""
			},
			index:    []interface{}{},
			expected: []interface{}{[]interface{}{"_uuid", "==", UUID{GoUUID: aUUID0}}},
			err:      false,
		},
		{
			name: "specify index",
			prepare: func(t *testType) {
				t.ID = aUUID0
				t.MyName = "foo"
				t.Config = nil
				t.Comp1 = ""
				t.Comp2 = ""
			},
			index:    []interface{}{&testObj.MyName},
			expected: []interface{}{[]interface{}{"name", "==", "foo"}},
			err:      false,
		},
		{
			name: "complex index",
			prepare: func(t *testType) {
				t.ID = ""
				t.MyName = ""
				t.Config = nil
				t.Comp1 = "foo"
				t.Comp2 = "bar"
			},
			expected: []interface{}{[]interface{}{"composed_1", "==", "foo"},
				[]interface{}{"composed_2", "==", "bar"}},
			index: []interface{}{},
			err:   false,
		},
		{
			name: "first index",
			prepare: func(t *testType) {
				t.ID = ""
				t.MyName = "something"
				t.Config = nil
				t.Comp1 = "foo"
				t.Comp2 = "bar"
			},
			expected: []interface{}{[]interface{}{"name", "==", "something"}},
			index:    []interface{}{},
			err:      false,
		},
		{
			name: "Error: None",
			prepare: func(t *testType) {
				t.ID = ""
				t.MyName = ""
				t.Config = map[string]string{"foo": "bar"}
				t.Comp1 = ""
				t.Comp2 = ""
			},
			index: []interface{}{},
			err:   true,
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("newCondition_%s", tt.name), func(t *testing.T) {
			tt.prepare(&testObj)
			conds, err := orm.newCondition("TestTable", &testObj, tt.index...)
			if tt.err {
				if err == nil {
					t.Errorf("Expected an error but got none")
				}
			} else {
				if err != nil {
					t.Error(err)
				}
				if !assert.ElementsMatch(t, tt.expected, conds, "Condition must match expected") {
					t.Logf("%v \n", conds)
				}
			}

		})
	}
}
