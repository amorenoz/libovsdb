package libovsdb

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
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
        "anotherSet": {
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

//
// When going Native -> OvS:
//	map -> *OvsMap
//	slice -> *OvsSet
// However, when going OvS -> Native
//	OvsMap -> map
//	OvsSet -> slice
// Perform indirection of ovs fields to be compared
// with the ones that wre used initially
func expectedOvs(in interface{}) interface{} {
	switch in.(type) {
	case *OvsSet:
		return *(in.(*OvsSet))
	case *OvsMap:
		return *(in.(*OvsMap))
	default:
		return in
	}
}
func getNativeMap() map[string]interface{} {
	return map[string]interface{}{
		"aString":  aString,
		"aSet":     aSet,
		"aUUIDSet": aUUIDSet,
		"aMap":     aMap,
		"aUUID":    aUUID0,
		"aIntSet":  aIntSet,
	}
}

func GetOvsRow() Row {
	ovsRow := Row{Fields: make(map[string]interface{})}
	ovsRow.Fields["aString"] = aString
	s, _ := NewOvsSet(aSet)
	ovsRow.Fields["aSet"] = *s

	us := make([]UUID, 0)
	for _, u := range aUUIDSet {
		us = append(us, UUID{GoUUID: u})
	}
	ovsUs, _ := NewOvsSet(us)
	ovsRow.Fields["aUUIDSet"] = *ovsUs
	m, _ := NewOvsMap(aMap)
	ovsRow.Fields["aMap"] = *m
	ovsRow.Fields["aUUID"] = UUID{GoUUID: aUUID0}
	is, e := NewOvsSet(aIntSet)
	if e != nil {
		fmt.Printf("%s", e.Error())
	}

	ovsRow.Fields["aIntSet"] = *is
	return ovsRow
}

func TestGetData(t *testing.T) {
	ovsRow := GetOvsRow()

	/* Code under test */
	var schema DatabaseSchema
	if err := json.Unmarshal(testSchema, &schema); err != nil {
		t.Error(err)
	}

	nf := NativeAPI{schema: schema}
	var data map[string]interface{}
	if err := nf.GetRowData("TestTable", &ovsRow, &data); err != nil {
		t.Error(err)
	}
	/* End: code under test */

	if len(data) != len(ovsRow.Fields) {
		t.Errorf("wrong length %d", len(data))
	}

	// Verify I can cast the content of data to native types
	if v, ok := data["aSet"].([]string); !ok || !reflect.DeepEqual(v, aSet) {
		t.Errorf("invalid set value %v", v)
	}
	if v, ok := data["aMap"].(map[string]string); !ok || !reflect.DeepEqual(v, aMap) {
		t.Errorf("invalid map value %v", v)
	}
	if v, ok := data["aUUIDSet"].([]string); !ok || !reflect.DeepEqual(v, aUUIDSet) {
		t.Errorf("invalid uuidset value %v", v)
	}
	if v, ok := data["aUUID"].(string); !ok || !reflect.DeepEqual(v, aUUID0) {
		t.Errorf("invalid uuidvalue %v", v)
	}
	if v, ok := data["aIntSet"].([]int); !ok || !reflect.DeepEqual(v, aIntSet) {
		t.Errorf("invalid integer set %v", v)
	}
}

func TestNewRow(t *testing.T) {
	ovsRow := GetOvsRow()

	/* Code under test */
	var schema DatabaseSchema
	if err := json.Unmarshal(testSchema, &schema); err != nil {
		t.Error(err)
	}
	nf := NativeAPI{schema: schema}
	row, err := nf.NewRow("TestTable", getNativeMap())
	if err != nil {
		t.Error(err)
	}

	for k := range row {
		if !reflect.DeepEqual(expectedOvs(row[k]), ovsRow.Fields[k]) {
			t.Errorf("Failed to convert to ovs. Key %s", k)
			fmt.Printf("value: %v\n", expectedOvs(row[k]))
			fmt.Printf("expected : %v\n", ovsRow.Fields[k])
		}

	}
}
