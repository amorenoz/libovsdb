package libovsdb

import (
	"encoding/json"
	"reflect"
	"testing"
)

var (
	aString = "foo"
	aEnum   = "enum1"
	aSet    = []string{"a", "set", "of", "strings"}
	aUUID0  = "2f77b348-9768-4866-b761-89d5177ecda0"
	aUUID1  = "2f77b348-9768-4866-b761-89d5177ecda1"
	aUUID2  = "2f77b348-9768-4866-b761-89d5177ecda2"
	aUUID3  = "2f77b348-9768-4866-b761-89d5177ecda3"

	aUUIDSet = []string{
		aUUID0,
		aUUID1,
		aUUID2,
		aUUID3,
	}

	aIntSet = []int{
		0,
		1,
		2,
		3,
	}
	aFloat = 42.00

	aFloatSet = []float64{
		3.14,
		2.71,
		42.0,
	}

	aMap = map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	aEmptySet = []string{}
)

func getColnames() []string {
	return []string{
		"aString",
		"aSet",
		"anotherSet",
		"aUUIDSet",
		"aIntSet",
		"aFloat",
		"aFloatSet",
		"aMap",
		"aUUID",
		"aEmptySet",
		"aEnum",
	}
}

func getTransMaps() []map[string]interface{} {
	var transMap []map[string]interface{}
	// String
	transMap = append(transMap, map[string]interface{}{
		"schema":     []byte(`{"type":"string"}`),
		"native":     aString,
		"native2ovs": aString,
		"ovs":        aString,
		"ovs2native": aString,
	})

	// Float
	transMap = append(transMap, map[string]interface{}{
		"schema":     []byte(`{"type":"real"}`),
		"native":     aFloat,
		"native2ovs": aFloat,
		"ovs":        aFloat,
		"ovs2native": aFloat,
	})

	// string set
	s, _ := NewOvsSet(aSet)
	transMap = append(transMap, map[string]interface{}{
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

	// A integer set
	is, _ := NewOvsSet(aIntSet)
	transMap = append(transMap, map[string]interface{}{
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

	// A float set
	fs, _ := NewOvsSet(aFloatSet)
	transMap = append(transMap, map[string]interface{}{
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

	// A Map
	m, _ := NewOvsMap(aMap)
	transMap = append(transMap, map[string]interface{}{
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
		var column ColumnSchema
		t.Logf("Testing %v", string(trans["schema"].([]byte)))
		if err := json.Unmarshal(trans["schema"].([]byte), &column); err != nil {
			t.Fatal(err)
		}

		res, err := OvsToNative(&column, trans["ovs"])
		if err != nil {
			t.Error(err)
		}

		if !reflect.DeepEqual(res, trans["ovs2native"]) {
			t.Errorf("Fail to convert ovs2native. OVS: %v(%s). Expected %v(%s). Got %v (%s)",
				trans["ovs"], reflect.TypeOf(trans["ovs"]),
				trans["ovs2native"], reflect.TypeOf(trans["ovs2native"]),
				res, reflect.TypeOf(res))
		}
	}
}

func TestNativeToOvs(t *testing.T) {
	transMaps := getTransMaps()
	for _, trans := range transMaps {
		t.Logf("Testing %v", string(trans["schema"].([]byte)))
		var column ColumnSchema
		if err := json.Unmarshal(trans["schema"].([]byte), &column); err != nil {
			t.Fatal(err)
		}

		res, err := NativeToOvs(&column, trans["native"])
		if err != nil {
			t.Error(err)
		}

		if !reflect.DeepEqual(res, trans["native2ovs"]) {
			t.Errorf("Fail to convert native2ovs. Native: %v(%s). Expected %v(%s). Got %v (%s)",
				trans["native"], reflect.TypeOf(trans["native"]),
				trans["native2ovs"], reflect.TypeOf(trans["native2ovs"]),
				res, reflect.TypeOf(res))
		}
	}
}
