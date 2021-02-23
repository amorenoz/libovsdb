package libovsdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	aString  = "foo"
	aEnum    = "enum1"
	aEnumSet = []string{"enum1", "enum2", "enum3"}
	aSet     = []string{"a", "set", "of", "strings"}
	aUUID0   = "2f77b348-9768-4866-b761-89d5177ecda0"
	aUUID1   = "2f77b348-9768-4866-b761-89d5177ecda1"
	aUUID2   = "2f77b348-9768-4866-b761-89d5177ecda2"
	aUUID3   = "2f77b348-9768-4866-b761-89d5177ecda3"

	aUUIDSet = []string{
		aUUID0,
		aUUID1,
		aUUID2,
		aUUID3,
	}

	aIntSet = []int{
		3,
		2,
		42,
	}
	aFloat = 42.00

	aInt = 42

	aFloatSet = []float64{
		3.0,
		2.0,
		42.0,
	}

	aMap = map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	aEmptySet = []string{}
)

func testOvsSet(t *testing.T, set interface{}) *OvsSet {
	oSet, err := NewOvsSet(set)
	assert.Nil(t, err)
	return oSet
}

func testOvsMap(t *testing.T, set interface{}) *OvsMap {
	oMap, err := NewOvsMap(set)
	assert.Nil(t, err)
	return oMap
}
