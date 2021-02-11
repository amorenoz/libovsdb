package libovsdb

import (
	"fmt"
	"reflect"
)

// nativeTypeFromExtended returns the native type that can hold a value of an
// Extended type
func nativeTypeFromExtended(extended ExtendedType) (reflect.Type, error) {
	switch extended {
	case TypeInteger:
		return reflect.TypeOf(0), nil
	case TypeReal:
		return reflect.TypeOf(0.0), nil
	case TypeBoolean:
		return reflect.TypeOf(true), nil
	case TypeString:
		return reflect.TypeOf(""), nil
	case TypeUUID:
		return reflect.TypeOf(""), nil
	default:
		return reflect.TypeOf(nil), fmt.Errorf("Failed to determine type of column %s", extended)
	}
}

// NativeValueOf returns the native value of the atomic element
// Usually, this is just reflect.ValueOf(elem), with the only exception of the UUID
func NativeValueOf(elem interface{}, elemType ExtendedType) (reflect.Value, error) {
	if elemType == TypeUUID {
		uuid, ok := elem.(UUID)
		if !ok {
			return reflect.ValueOf(nil), fmt.Errorf("Element in should be convertible to UUID. Instead got %s", reflect.TypeOf(elem))
		}
		return reflect.ValueOf(uuid.GoUUID), nil
	}
	return reflect.ValueOf(elem), nil

}

//NativeType returns the reflect.Type that can hold the value of a column
//OVS Type to Native Type convertions:
// OVS sets -> go slices
// OVS uuid -> go strings
// OVS map  -> go map
// OVS enum -> go native type depending on the type of the enum key
func NativeType(column *ColumnSchema) (reflect.Type, error) {
	switch column.Type {
	case TypeInteger, TypeReal, TypeBoolean, TypeUUID, TypeString:
		return nativeTypeFromExtended(column.Type)
	case TypeEnum:
		return nativeTypeFromExtended(column.TypeObj.Key.Type)
	case TypeMap:
		kType, err := nativeTypeFromExtended(column.TypeObj.Key.Type)
		if err != nil {
			return reflect.TypeOf(nil), err
		}
		vType, err := nativeTypeFromExtended(column.TypeObj.Value.Type)
		if err != nil {
			return reflect.TypeOf(nil), err
		}
		return reflect.MapOf(kType, vType), nil
	case TypeSet:
		kType, err := nativeTypeFromExtended(column.TypeObj.Key.Type)
		if err != nil {
			return reflect.TypeOf(nil), err
		}
		return reflect.SliceOf(kType), nil
	default:
		return reflect.TypeOf(nil), fmt.Errorf("Failed to determine type of column %s", column)
	}
}

// OvsToNative transforms an ovs type to native one based on the column type information
func OvsToNative(column *ColumnSchema, ovsElem interface{}) (interface{}, error) {
	naType, err := NativeType(column)
	if err != nil {
		return nil, err
	}
	switch column.Type {
	case TypeInteger, TypeReal, TypeString, TypeBoolean, TypeEnum:
		// Atomic types should have the same underlying type
		return ovsElem, nil
	case TypeUUID:
		uuid, ok := ovsElem.(UUID)
		if !ok {
			return nil, fmt.Errorf("Element in column should be convertible to UUID. Instead got %s", reflect.TypeOf(ovsElem))
		}
		return uuid.GoUUID, nil
	case TypeSet:
		ovsSet, ok := ovsElem.(OvsSet)
		if !ok {
			return nil, fmt.Errorf("Element in column should be convertible to Set. Instead got %s", reflect.TypeOf(ovsElem))
		}
		// The inner slice is []interface{}
		// We need to convert it to the real type os slice
		nativeSet := reflect.MakeSlice(naType, 0, 0)
		for _, v := range ovsSet.GoSet {
			vv, err := NativeValueOf(v, column.TypeObj.Key.Type)
			if err != nil {
				return nil, err
			}
			nativeSet = reflect.Append(nativeSet, vv)
		}
		return nativeSet.Interface(), nil
	case TypeMap:
		ovsMap, ok := ovsElem.(OvsMap)
		if !ok {
			return nil, fmt.Errorf("Element in column should be convertible to Map. Instead got %s", reflect.TypeOf(ovsElem))
		}
		// The inner slice is map[interface]interface{}
		// We need to convert it to the real type os slice
		nativeMap := reflect.MakeMap(naType)
		for k, v := range ovsMap.GoMap {
			kk, err := NativeValueOf(k, column.TypeObj.Key.Type)
			if err != nil {
				return nil, err
			}
			vv, err := NativeValueOf(v, column.TypeObj.Value.Type)
			if err != nil {
				return nil, err
			}
			nativeMap.SetMapIndex(kk, vv)
		}
		return nativeMap.Interface(), nil
	default:
		return nil, fmt.Errorf("Unknown type %s", column.Type)
	}
}

// NativeToOvs transforms an native type to a ovs type based on the column type information
func NativeToOvs(column *ColumnSchema, rawElem interface{}) (interface{}, error) {
	// Type Validation
	naType, err := NativeType(column)
	if err != nil {
		return nil, err
	}
	if t := reflect.TypeOf(rawElem); t != naType {
		return nil, fmt.Errorf("Bad Type in column expected %s, got %s (%v)",
			naType.String(), t.String(), rawElem)
	}
	switch column.Type {
	case TypeInteger, TypeReal, TypeString, TypeBoolean, TypeEnum:
		return rawElem, nil
	case TypeUUID:
		return UUID{GoUUID: rawElem.(string)}, nil
	case TypeSet:
		var ovsSet *OvsSet
		if column.TypeObj.Key.Type == TypeUUID {
			var ovsSlice []interface{}
			for _, v := range rawElem.([]string) {
				uuid := UUID{GoUUID: v}
				ovsSlice = append(ovsSlice, uuid)
			}
			ovsSet = &OvsSet{GoSet: ovsSlice}

		} else {
			ovsSet, err = NewOvsSet(rawElem)
			if err != nil {
				return nil, err
			}
		}
		return ovsSet, nil
	case TypeMap:
		ovsMap, err := NewOvsMap(rawElem)
		if err != nil {
			return nil, err
		}
		return ovsMap, nil
	default:
		return nil, fmt.Errorf("Unsuppored type %s", column.Type)
	}
}
