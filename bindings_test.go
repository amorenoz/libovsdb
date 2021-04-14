package libovsdb

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

type Transformation map[string]interface{}

func (t Transformation) String() string {
	return fmt.Sprintf("Transformation: \n\t schema: %s\n\t native: %v\n\t native2ovs: %v\n\t ovs: %v\n\t ovs2native: %v\n\t",
		string(t["schema"].([]byte)), t["native"], t["native2ovs"], t["ovs"], t["ovs2native"])

}

func getErrTransMaps() []map[string]interface{} {
	var transMap []map[string]interface{}
	transMap = append(transMap, map[string]interface{}{
		"name":   "Wrong Atomic Type",
		"schema": []byte(`{"type":"string"}`),
		"native": 42,
		"ovs":    42,
	})

	// OVS floats should be convertible to integers since encoding/json will use float64 as
	// the default numeric type. However, native types should match
	transMap = append(transMap, map[string]interface{}{
		"name":   "Wrong Atomic Numeric Type: Int",
		"schema": []byte(`{"type":"integer"}`),
		"native": 42.0,
	})

	transMap = append(transMap, map[string]interface{}{
		"name":   "Wrong Atomic Numeric Type: Float",
		"schema": []byte(`{"type":"real"}`),
		"native": 42,
		"ovs":    42,
	})
	as, _ := NewOvsSet([]string{"foo"})
	transMap = append(transMap, map[string]interface{}{
		"name":   "Set instead of Atomic Type",
		"schema": []byte(`{"type":"string"}`),
		"native": []string{"foo"},
		"ovs":    *as,
	})
	transMap = append(transMap, map[string]interface{}{
		"name": "Wrong Set Type",
		"schema": []byte(`{
          "type": {
            "key": "string",
            "max": "unlimited",
            "min": 0
          }
        }`),
		"native": []int{1, 2},
		"ovs":    []int{1, 2},
	})

	s, _ := NewOvsMap(map[string]string{"foo": "bar"})
	transMap = append(transMap, map[string]interface{}{
		"name": "Wrong Map instead of Set",
		"schema": []byte(`{
          "type": {
            "key": "string",
            "max": "unlimited",
            "min": 0
          }
        }`),
		"native": map[string]string{"foo": "bar"},
		"ovs":    *s,
	})

	m, _ := NewOvsMap(map[int]string{1: "one", 2: "two"})
	transMap = append(transMap, map[string]interface{}{
		"name": "Wrong Map key type",
		"schema": []byte(`{
          "type": {
            "key": "string",
            "max": "unlimited",
            "min": 0,
            "value": "string"
          }
	}`),
		"native": map[int]string{1: "one", 2: "two"},
		"ovs":    *m,
	})
	return transMap
}
func getTransMaps() []map[string]interface{} {
	var transMap []map[string]interface{}
	// String
	transMap = append(transMap, map[string]interface{}{
		"name":       "String",
		"schema":     []byte(`{"type":"string"}`),
		"native":     aString,
		"native2ovs": aString,
		"ovs":        aString,
		"ovs2native": aString,
	})

	// Float
	transMap = append(transMap, map[string]interface{}{
		"name":       "Float",
		"schema":     []byte(`{"type":"real"}`),
		"native":     aFloat,
		"native2ovs": aFloat,
		"ovs":        aFloat,
		"ovs2native": aFloat,
	})

	// Integers
	transMap = append(transMap, map[string]interface{}{
		"name":       "Integers with float ovs type",
		"schema":     []byte(`{"type":"integer"}`),
		"native":     aInt,
		"native2ovs": aInt,
		"ovs":        aFloat, // Default json encoding uses float64 for all numbers
		"ovs2native": aInt,
	})
	transMap = append(transMap, map[string]interface{}{
		"name":       "Integers",
		"schema":     []byte(`{"type":"integer"}`),
		"native":     aInt,
		"native2ovs": aInt,
		"ovs":        aInt,
		"ovs2native": aInt,
	})
	transMap = append(transMap, map[string]interface{}{
		"name":       "Integer set with float ovs type ",
		"schema":     []byte(`{"type":"integer", "min":0}`),
		"native":     aInt,
		"native2ovs": aInt,
		"ovs":        aFloat,
		"ovs2native": aInt,
	})

	// string set
	s, _ := NewOvsSet(aSet)
	transMap = append(transMap, map[string]interface{}{
		"name": "String Set",
		"schema": []byte(`{
          "type": {
            "key": "string",
            "max": "unlimited",
            "min": 0
          }
        }`),
		"native":     aSet,
		"native2ovs": s,
		"ovs":        *s,
		"ovs2native": aSet,
	})

	// string with exactly one element can also be represented
	// as the element itself. On ovs2native, we keep the slice representation
	s1, _ := NewOvsSet([]string{aString})
	transMap = append(transMap, map[string]interface{}{
		"name": "String Set with exactly one field",
		"schema": []byte(`{
          "type": {
            "key": "string",
            "max": "unlimited",
            "min": 0
          }
        }`),
		"native":     []string{aString},
		"native2ovs": s1,
		"ovs":        aString,
		"ovs2native": []string{aString},
	})

	// UUID set
	us := make([]UUID, 0)
	for _, u := range aUUIDSet {
		us = append(us, UUID{GoUUID: u})
	}
	uss, _ := NewOvsSet(us)
	transMap = append(transMap, map[string]interface{}{
		"name": "UUID Set",
		"schema": []byte(`{
	"type":{
            "key": {
              "refTable": "SomeOtherTAble",
              "refType": "weak",
              "type": "uuid"
            },
            "min": 0
         }
	}`),
		"native":     aUUIDSet,
		"native2ovs": uss,
		"ovs":        *uss,
		"ovs2native": aUUIDSet,
	})

	// UUID set with exactly one element.
	us1 := []UUID{{GoUUID: aUUID0}}
	uss1, _ := NewOvsSet(us1)
	transMap = append(transMap, map[string]interface{}{
		"name": "UUID Set with exactly one field",
		"schema": []byte(`{
	"type":{
            "key": {
              "refTable": "SomeOtherTAble",
              "refType": "weak",
              "type": "uuid"
            },
            "min": 0
         }
	}`),
		"native":     []string{aUUID0},
		"native2ovs": uss1,
		"ovs":        UUID{GoUUID: aUUID0},
		"ovs2native": []string{aUUID0},
	})

	// A integer set with integer ovs input
	is, _ := NewOvsSet(aIntSet)
	fs, _ := NewOvsSet(aFloatSet)
	transMap = append(transMap, map[string]interface{}{
		"name": "Integer Set",
		"schema": []byte(`{
	"type":{
            "key": {
              "type": "integer"
            },
            "min": 0,
            "max": "unlimited"
          }
        }`),
		"native":     aIntSet,
		"native2ovs": is,
		"ovs":        *is,
		"ovs2native": aIntSet,
	})

	// A integer set with float ovs input
	transMap = append(transMap, map[string]interface{}{
		"name": "Integer Set single",
		"schema": []byte(`{
	"type":{
            "key": {
              "type": "integer"
            },
            "min": 0,
            "max": "unlimited"
          }
        }`),
		"native":     aIntSet,
		"native2ovs": is,
		"ovs":        *fs,
		"ovs2native": aIntSet,
	})

	// A single-value integer set with integer ovs input
	sis, _ := NewOvsSet([]int{aInt})
	sfs, _ := NewOvsSet([]float64{aFloat})
	transMap = append(transMap, map[string]interface{}{
		"name": "Integer Set single",
		"schema": []byte(`{
	"type":{
            "key": {
              "type": "integer"
            },
            "min": 0,
            "max": "unlimited"
          }
        }`),
		"native":     []int{aInt},
		"native2ovs": sis,
		"ovs":        *sis,
		"ovs2native": []int{aInt},
	})

	// A single-value integer set with float ovs input
	transMap = append(transMap, map[string]interface{}{
		"name": "Integer Set single",
		"schema": []byte(`{
	"type":{
            "key": {
              "type": "integer"
            },
            "min": 0,
            "max": "unlimited"
          }
        }`),
		"native":     []int{aInt},
		"native2ovs": sis,
		"ovs":        *sfs,
		"ovs2native": []int{aInt},
	})

	// A float set
	transMap = append(transMap, map[string]interface{}{
		"name": "Float Set",
		"schema": []byte(`{
	"type":{
            "key": {
              "type": "real"
            },
            "min": 0,
            "max": "unlimited"
          }
        }`),
		"native":     aFloatSet,
		"native2ovs": fs,
		"ovs":        *fs,
		"ovs2native": aFloatSet,
	})

	// A empty string set
	es, _ := NewOvsSet(aEmptySet)
	transMap = append(transMap, map[string]interface{}{
		"name": "Empty String Set",
		"schema": []byte(`{
	"type":{
            "key": {
              "type": "string"
            },
            "min": 0,
            "max": "unlimited"
          }
        }`),
		"native":     aEmptySet,
		"native2ovs": es,
		"ovs":        *es,
		"ovs2native": aEmptySet,
	})

	// Enum
	transMap = append(transMap, map[string]interface{}{
		"name": "Enum (string)",
		"schema": []byte(`{
	"type":{
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
	}`),
		"native":     aEnum,
		"native2ovs": aEnum,
		"ovs":        aEnum,
		"ovs2native": aEnum,
	})

	// Enum set
	ens, _ := NewOvsSet(aEnumSet)
	transMap = append(transMap, map[string]interface{}{
		"name": "Enum Set (string)",
		"schema": []byte(`{
	"type":{
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
            },
	    "max": "unlimited",
	    "min": 0
          }
	}`),
		"native":     aEnumSet,
		"native2ovs": ens,
		"ovs":        *ens,
		"ovs2native": aEnumSet,
	})

	// A Map
	m, _ := NewOvsMap(aMap)
	transMap = append(transMap, map[string]interface{}{
		"name": "Map (string->string)",
		"schema": []byte(`{
          "type": {
            "key": "string",
            "max": "unlimited",
            "min": 0,
            "value": "string"
          }
	}`),
		"native":     aMap,
		"native2ovs": m,
		"ovs":        *m,
		"ovs2native": aMap,
	})
	return transMap
}

func TestOvsToNative(t *testing.T) {
	transMaps := getTransMaps()
	for _, trans := range transMaps {
		t.Run(fmt.Sprintf("Ovs To Native: %s", trans["name"]), func(t *testing.T) {
			var column ColumnSchema
			if err := json.Unmarshal(trans["schema"].([]byte), &column); err != nil {
				t.Fatal(err)
			}

			res, err := OvsToNative(&column, trans["ovs"])
			if err != nil {
				t.Errorf("Failed to convert %s: %s", trans, err)
				t.Logf("Testing %v", string(trans["schema"].([]byte)))
			}

			if !reflect.DeepEqual(res, trans["ovs2native"]) {
				t.Errorf("Fail to convert ovs2native. OVS: %v(%s). Expected %v(%s). Got %v (%s)",
					trans["ovs"], reflect.TypeOf(trans["ovs"]),
					trans["ovs2native"], reflect.TypeOf(trans["ovs2native"]),
					res, reflect.TypeOf(res))
			}
		})
	}
}

func TestNativeToOvs(t *testing.T) {
	transMaps := getTransMaps()
	for _, trans := range transMaps {
		t.Run(fmt.Sprintf("Native To Ovs: %s", trans["name"]), func(t *testing.T) {
			var column ColumnSchema
			if err := json.Unmarshal(trans["schema"].([]byte), &column); err != nil {
				t.Fatal(err)
			}

			res, err := NativeToOvs(&column, trans["native"])
			if err != nil {
				t.Errorf("Failed to convert %s: %s", trans, err)
				t.Logf("Testing %v", string(trans["schema"].([]byte)))
			}

			if !reflect.DeepEqual(res, trans["native2ovs"]) {
				t.Errorf("Fail to convert native2ovs. Native: %v(%s). Expected %v(%s). Got %v (%s)",
					trans["native"], reflect.TypeOf(trans["native"]),
					trans["native2ovs"], reflect.TypeOf(trans["native2ovs"]),
					res, reflect.TypeOf(res))
			}
		})
	}
}

func TestOvsToNativeErr(t *testing.T) {
	transMaps := getErrTransMaps()
	for _, trans := range transMaps {
		if _, ok := trans["ovs"]; !ok {
			continue
		}
		t.Run(fmt.Sprintf("Ovs To Native Error: %s", trans["name"]), func(t *testing.T) {
			var column ColumnSchema
			if err := json.Unmarshal(trans["schema"].([]byte), &column); err != nil {
				t.Fatal(err)
			}

			res, err := OvsToNative(&column, trans["ovs"])
			if err == nil {
				t.Errorf("Convertion %s should have failed, instead it has returned %v (%s)", trans, res, reflect.TypeOf(res))
				t.Logf("Conversion schema %v", string(trans["schema"].([]byte)))
			}
		})
	}
}

func TestNativeToOvsErr(t *testing.T) {
	transMaps := getErrTransMaps()
	for _, trans := range transMaps {
		if _, ok := trans["native"]; !ok {
			continue
		}
		t.Run(fmt.Sprintf("Native To Ovs Error: %s", trans["name"]), func(t *testing.T) {
			var column ColumnSchema
			if err := json.Unmarshal(trans["schema"].([]byte), &column); err != nil {
				t.Fatal(err)
			}

			res, err := NativeToOvs(&column, trans["native"])
			if err == nil {
				t.Errorf("Convertion %s should have failed, instead it has returned %v (%s)", trans, res, reflect.TypeOf(res))
				t.Logf("Conversion schema %v", string(trans["schema"].([]byte)))
			}
		})
	}
}

func TestIsDefault(t *testing.T) {
	type Test struct {
		name     string
		column   []byte
		elem     interface{}
		expected bool
	}
	tests := []Test{
		{
			name:     "empty string",
			column:   []byte(`{"type":"string"}`),
			elem:     "",
			expected: true,
		},
		{
			name:     "non string",
			column:   []byte(`{"type":"string"}`),
			elem:     "something",
			expected: false,
		},
		{
			name:     "empty uuid",
			column:   []byte(`{"type":"uuid"}`),
			elem:     "",
			expected: true,
		},
		{
			name:     "default uuid",
			column:   []byte(`{"type":"uuid"}`),
			elem:     "00000000-0000-0000-0000-000000000000",
			expected: true,
		},
		{
			name:     "non-empty uuid",
			column:   []byte(`{"type":"uuid"}`),
			elem:     aUUID0,
			expected: false,
		},
		{
			name:     "zero int",
			column:   []byte(`{"type":"integer"}`),
			elem:     0,
			expected: true,
		},
		{
			name:     "non-zero int",
			column:   []byte(`{"type":"integer"}`),
			elem:     42,
			expected: false,
		},
		{
			name:     "non-zero float",
			column:   []byte(`{"type":"real"}`),
			elem:     42.0,
			expected: false,
		},
		{
			name:     "zero float",
			column:   []byte(`{"type":"real"}`),
			elem:     0.0,
			expected: true,
		},
		{
			name: "empty set ",
			column: []byte(`{
					   "type": {
   					     "key": "string",
    					     "max": "unlimited",
    					     "min": 0
    					   }
    					 }`),
			elem:     []string{},
			expected: true,
		},
		{
			name: "empty set allocated",
			column: []byte(`{
					   "type": {
    					     "key": "string",
    					     "max": "unlimited",
    					     "min": 0
    					   }
    					 }`),
			elem:     make([]string, 0, 10),
			expected: true,
		},
		{
			name: "non-empty set",
			column: []byte(`{
					   "type": {
    					     "key": "string",
    					     "max": "unlimited",
    					     "min": 0
    					   }
    					 }`),
			elem:     []string{"something"},
			expected: false,
		},
		{
			name: "empty map allocated",
			column: []byte(`{
					   "type": {
    					     "key": "string",
    					     "value": "string"
    					   }
    					 }`),
			elem:     make(map[string]string),
			expected: true,
		},
		{
			name: "nil map",
			column: []byte(`{
					   "type": {
    					     "key": "string",
    					     "value": "string"
    					   }
    					 }`),
			elem:     nil,
			expected: true,
		},
		{
			name: "empty map",
			column: []byte(`{
					   "type": {
    					     "key": "string",
    					     "value": "string"
    					   }
    					 }`),
			elem:     map[string]string{},
			expected: true,
		},
		{
			name: "non-empty map",
			column: []byte(`{
					   "type": {
    					     "key": "string",
    					     "value": "string"
    					   }
   					 }`),
			elem:     map[string]string{"foo": "bar"},
			expected: false,
		},
		{
			name: "empty enum",
			column: []byte(`{
					"type":{
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
					}`),
			elem:     "",
			expected: true,
		},
		{
			name: "non-empty enum",
			column: []byte(`{
					"type":{
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
					}`),
			elem:     "enum1",
			expected: false,
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("IsDefault: %s", test.name), func(t *testing.T) {
			var column ColumnSchema
			if err := json.Unmarshal(test.column, &column); err != nil {
				t.Fatal(err)
			}

			result := IsDefaultValue(&column, test.elem)
			if result != test.expected {
				t.Errorf("Failed to determine if %v is default. Expected %t got %t", test, test.expected, result)
				t.Logf("Conversion schema %v", string(test.column))
			}

		})
	}
}

func TestMutationValidation(t *testing.T) {
	type Test struct {
		name     string
		column   []byte
		mutators []string
		value    interface{}
		valid    bool
	}
	tests := []Test{
		{
			name:     "string",
			column:   []byte(`{"type":"string"}`),
			mutators: []string{"+=", "+=", "-=", "*=", "/=", "%="},
			value:    "foo",
			valid:    false,
		},
		{
			name:     "string",
			column:   []byte(`{"type":"uuid"}`),
			mutators: []string{"+=", "+=", "-=", "*=", "/=", "%="},
			value:    "foo",
			valid:    false,
		},
		{
			name:     "boolean",
			column:   []byte(`{"type":"boolean"}`),
			mutators: []string{"+=", "+=", "-=", "*=", "/=", "%="},
			value:    true,
			valid:    false,
		},
		{
			name:     "integer",
			column:   []byte(`{"type":"integer"}`),
			mutators: []string{"+=", "+=", "-=", "*=", "/=", "%="},
			value:    4,
			valid:    true,
		},
		{
			name:     "unmutable",
			column:   []byte(`{"type":"integer", "mutable": false}`),
			mutators: []string{"+=", "+=", "-=", "*=", "/=", "%="},
			value:    4,
			valid:    false,
		},
		{
			name:     "integer wrong mutator",
			column:   []byte(`{"type":"integer"}`),
			mutators: []string{"some", "wrong", "mutator"},
			value:    4,
			valid:    false,
		},
		{
			name:     "integer wrong type",
			column:   []byte(`{"type":"integer"}`),
			mutators: []string{"+=", "+=", "-=", "*=", "/=", "%="},
			value:    "foo",
			valid:    false,
		},
		{
			name:     "real",
			column:   []byte(`{"type":"real"}`),
			mutators: []string{"+=", "+=", "-=", "*=", "/="},
			value:    4.0,
			valid:    true,
		},
		{
			name:     "real-%/",
			column:   []byte(`{"type":"real"}`),
			mutators: []string{"%="},
			value:    4.0,
			valid:    false,
		},
		{
			name: "integer set",
			column: []byte(`{
				   "type": {
				     "key": "integer",
				     "max": "unlimited",
				     "min": 0
				   }
				 }`),
			mutators: []string{"+=", "+=", "-=", "*=", "/=", "%="},
			value:    4,
			valid:    true,
		},
		{
			name: "float set /",
			column: []byte(`{
				   "type": {
				     "key": "real",
				     "max": "unlimited",
				     "min": 0
				   }
				 }`),
			mutators: []string{"+=", "+=", "-=", "*=", "/="},
			value:    4.0,
			valid:    true,
		},
		{
			name: "string set wrong mutator",
			column: []byte(`{
				   "type": {
				     "key": "real",
				     "max": "unlimited",
				     "min": 0
				   }
				 }`),
			mutators: []string{"+=", "+=", "-=", "*="},
			value:    "foo",
			valid:    false,
		},
		{
			name: "string set insert/delete",
			column: []byte(`{
				   "type": {
				     "key": "string",
				     "max": "unlimited",
				     "min": 0
				   }
				 }`),
			mutators: []string{"insert", "delete"},
			value:    []string{"foo", "bar"},
			valid:    true,
		},
		{
			name: "integer set insert/delete",
			column: []byte(`{
				   "type": {
				     "key": "integer",
				     "max": "unlimited",
				     "min": 0
				   }
				 }`),
			mutators: []string{"insert", "delete"},
			value:    []int{45, 11},
			valid:    true,
		},
		{
			name: "map insert, wrong type",
			column: []byte(`{
				   "type": {
				     "key": "string",
				     "value": "string"
				   }
				 }`),
			mutators: []string{"insert"},
			value:    []string{"foo"},
			valid:    false,
		},
		{
			name: "map insert",
			column: []byte(`{
				   "type": {
				     "key": "string",
				     "value": "string"
				   }
				 }`),
			mutators: []string{"insert"},
			value:    map[string]string{"foo": "bar"},
			valid:    true,
		},
		{
			name: "map delete k-v",
			column: []byte(`{
				   "type": {
				     "key": "string",
				     "value": "string"
				   }
				 }`),
			mutators: []string{"delete"},
			value:    map[string]string{"foo": "bar"},
			valid:    true,
		},
		{
			name: "map delete k set",
			column: []byte(`{
				   "type": {
				     "key": "string",
				     "value": "string"
				   }
				 }`),
			mutators: []string{"delete"},
			value:    []string{"foo", "bar"},
			valid:    true,
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("MutationValidation: %s", test.name), func(t *testing.T) {
			var column ColumnSchema
			if err := json.Unmarshal(test.column, &column); err != nil {
				t.Fatal(err)
			}

			for _, m := range test.mutators {
				result := validateMutation(&column, m, test.value)
				if test.valid {
					assert.Nil(t, result)
				} else {
					assert.NotNil(t, result)
				}
			}
		})
	}
}
