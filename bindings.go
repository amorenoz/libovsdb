package libovsdb

import (
	"fmt"
	"reflect"
)

var (
	intType  = reflect.TypeOf(0)
	realType = reflect.TypeOf(0.0)
	boolType = reflect.TypeOf(true)
	strType  = reflect.TypeOf("")
)

// ErrWrongType describes typing error
type ErrWrongType struct {
	from     string
	expected string
	got      interface{}
}

func (e *ErrWrongType) Error() string {
	return fmt.Sprintf("Wrong Type (%s): expected %s but got %s (%s)",
		e.from, e.expected, e.got, reflect.TypeOf(e.got))
}

// NewErrWrongType creates a new ErrWrongType
func NewErrWrongType(from, expected string, got interface{}) error {
	return &ErrWrongType{
		from:     from,
		expected: expected,
		got:      got,
	}
}

// nativeTypeFromAtomic returns the native type that can hold a value of an
// AtomicType
func nativeTypeFromAtomic(basicType string) reflect.Type {
	switch basicType {
	case TypeInteger:
		return intType
	case TypeReal:
		return realType
	case TypeBoolean:
		return boolType
	case TypeString:
		return strType
	case TypeUUID:
		return strType
	default:
		panic("Unkown basic type %s basicType")
	}
}

//nativeType returns the reflect.Type that can hold the value of a column
//OVS Type to Native Type convertions:
// OVS sets -> go slices
// OVS uuid -> go strings
// OVS map  -> go map
// OVS enum -> go native type depending on the type of the enum key
func nativeType(column *ColumnSchema) reflect.Type {
	switch column.Type {
	case TypeInteger, TypeReal, TypeBoolean, TypeUUID, TypeString:
		return nativeTypeFromAtomic(column.Type)
	case TypeEnum:
		return nativeTypeFromAtomic(column.TypeObj.Key.Type)
	case TypeMap:
		kType := nativeTypeFromAtomic(column.TypeObj.Key.Type)
		vType := nativeTypeFromAtomic(column.TypeObj.Value.Type)
		return reflect.MapOf(kType, vType)
	case TypeSet:
		kType := nativeTypeFromAtomic(column.TypeObj.Key.Type)
		return reflect.SliceOf(kType)
	default:
		panic(fmt.Errorf("unknown extended type %s", column.Type))
	}
}

// OvsToNativeAtomic returns the native type of the basic ovs type
func OvsToNativeAtomic(basicType string, ovsElem interface{}) (interface{}, error) {
	switch basicType {
	case TypeReal, TypeString, TypeBoolean:
		naType := nativeTypeFromAtomic(basicType)
		if reflect.TypeOf(ovsElem) != naType {
			return nil, NewErrWrongType("OvsToNativeAtomic", naType.String(), ovsElem)
		}
		return ovsElem, nil
	case TypeInteger:
		naType := nativeTypeFromAtomic(basicType)
		// Default decoding of numbers is float64, convert them to int
		if !reflect.TypeOf(ovsElem).ConvertibleTo(naType) {
			return nil, NewErrWrongType("OvsToNativeAtomic", fmt.Sprintf("Convertible to %s", naType), ovsElem)
		}
		return reflect.ValueOf(ovsElem).Convert(naType).Interface(), nil
	case TypeUUID:
		uuid, ok := ovsElem.(UUID)
		if !ok {
			return nil, NewErrWrongType("OvsToNativeAtomic", "UUID", ovsElem)
		}
		return uuid.GoUUID, nil
	default:
		panic(fmt.Errorf("unknown atomic type %s", basicType))
	}
}

// OvsToNative transforms an ovs type to native one based on the column type information
func OvsToNative(column *ColumnSchema, ovsElem interface{}) (interface{}, error) {
	switch column.Type {
	case TypeReal, TypeString, TypeBoolean, TypeInteger, TypeUUID:
		return OvsToNativeAtomic(column.Type, ovsElem)
	case TypeEnum:
		return OvsToNativeAtomic(column.TypeObj.Key.Type, ovsElem)
	case TypeSet:
		naType := nativeType(column)
		// The inner slice is []interface{}
		// We need to convert it to the real type os slice
		var nativeSet reflect.Value

		// RFC says that for a set of exactly one, an atomic type an be sent
		switch ovsSet := ovsElem.(type) {
		case OvsSet:
			nativeSet = reflect.MakeSlice(naType, 0, len(ovsSet.GoSet))
			for _, v := range ovsSet.GoSet {
				nv, err := OvsToNativeAtomic(column.TypeObj.Key.Type, v)
				if err != nil {
					return nil, err
				}
				nativeSet = reflect.Append(nativeSet, reflect.ValueOf(nv))
			}

		default:
			nativeSet = reflect.MakeSlice(naType, 0, 1)
			nv, err := OvsToNativeAtomic(column.TypeObj.Key.Type, ovsElem)
			if err != nil {
				return nil, err
			}

			nativeSet = reflect.Append(nativeSet, reflect.ValueOf(nv))
		}
		return nativeSet.Interface(), nil

	case TypeMap:
		naType := nativeType(column)
		ovsMap, ok := ovsElem.(OvsMap)
		if !ok {
			return nil, NewErrWrongType("OvsToNative", "OvsMap", ovsElem)
		}
		// The inner slice is map[interface]interface{}
		// We need to convert it to the real type os slice
		nativeMap := reflect.MakeMapWithSize(naType, len(ovsMap.GoMap))
		for k, v := range ovsMap.GoMap {
			nk, err := OvsToNativeAtomic(column.TypeObj.Key.Type, k)
			if err != nil {
				return nil, err
			}
			nv, err := OvsToNativeAtomic(column.TypeObj.Value.Type, v)
			if err != nil {
				return nil, err
			}
			nativeMap.SetMapIndex(reflect.ValueOf(nk), reflect.ValueOf(nv))
		}
		return nativeMap.Interface(), nil
	default:
		panic(fmt.Sprintf("Unknown Type: %v", column.Type))
	}
}

// NativeToOvs transforms an native type to a ovs type based on the column type information
func NativeToOvs(column *ColumnSchema, rawElem interface{}) (interface{}, error) {
	naType := nativeType(column)

	if t := reflect.TypeOf(rawElem); t != naType {
		return nil, NewErrWrongType("NativeToOvs", naType.String(), rawElem)
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
			var err error
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
		panic(fmt.Sprintf("Unknown Type: %v", column.Type))
	}
}

// IsDefaultValue checks if a provided native element corresponds to the default value of its
// designated column type
func IsDefaultValue(column *ColumnSchema, nativeElem interface{}) bool {
	switch column.Type {
	case TypeEnum:
		return isDefaultBaseValue(nativeElem, column.TypeObj.Key.Type)
	default:
		return isDefaultBaseValue(nativeElem, column.Type)
	}
}

// validateMutator checks if the mutation is valid for a specific AtomicType
func validateMutationAtomic(atype, mutator string, value interface{}) error {
	nType := nativeTypeFromAtomic(atype)
	if reflect.TypeOf(value) != nType {
		return NewErrWrongType(fmt.Sprintf("Mutation of atomic type %s", atype), nType.String(), value)
	}

	switch atype {
	case TypeUUID, TypeString, TypeBoolean:
		return fmt.Errorf("AtomicType %s does not support mutation", atype)
	case TypeReal:
		switch mutator {
		case "+=", "-=", "*=", "/=":
			return nil
		default:
			return fmt.Errorf("Wrong mutator for real type %s", mutator)
		}
	case TypeInteger:
		switch mutator {
		case "+=", "-=", "*=", "/=", "%=":
			return nil
		default:
			return fmt.Errorf("Wrong mutator for integer type: %s", mutator)
		}
	default:
		panic("Unsupported Atomic Type")
	}
}

// ValidateMutation checks if the mutation value and mutator string area apropriate
// for a given column based on the rules specified RFC7047
func validateMutation(column *ColumnSchema, mutator string, value interface{}) error {
	if !column.Mutable {
		return fmt.Errorf("Column is not mutable")
	}
	switch column.Type {
	case TypeSet:
		switch mutator {
		case "insert", "delete":
			if nativeType(column) != reflect.TypeOf(value) {
				return NewErrWrongType(fmt.Sprintf("Mutation %s of column %s", mutator, column),
					nativeType(column).String(), value)
			}
			return nil
		default:
			return validateMutationAtomic(column.TypeObj.Key.Type, mutator, value)
		}
	case TypeMap:
		switch mutator {
		case "insert":
			// Value must be a map of the same kind
			if reflect.TypeOf(value) != nativeType(column) {
				return NewErrWrongType(fmt.Sprintf("Mutation %s of column %s", mutator, column),
					nativeType(column).String(), value)
			}
			return nil
		case "delete":
			// Value must be a map of the same kind or a set of keys to delete
			if reflect.TypeOf(value) != nativeType(column) &&
				reflect.TypeOf(value) != reflect.SliceOf(nativeTypeFromAtomic(column.TypeObj.Key.Type)) {
				return NewErrWrongType(fmt.Sprintf("Mutation %s of column %s", mutator, column),
					"compatible map type", value)
			}
			return nil

		default:
			return fmt.Errorf("Wrong mutator for map type: %s", mutator)
		}
	case TypeEnum:
		// RFC does not clarify what to do with enums.
		return fmt.Errorf("Enums do not support mutation")
	default:
		return validateMutationAtomic(column.Type, mutator, value)
	}
}

func isDefaultBaseValue(elem interface{}, etype ExtendedType) bool {
	value := reflect.ValueOf(elem)
	if !value.IsValid() {
		return true
	}

	switch etype {
	case TypeUUID:
		return elem.(string) == "00000000-0000-0000-0000-000000000000" || elem.(string) == ""
	case TypeMap, TypeSet:
		return value.IsNil() || value.Len() == 0
	case TypeString:
		return elem.(string) == ""
	case TypeInteger:
		return elem.(int) == 0
	case TypeReal:
		return elem.(float64) == 0
	default:
		return false
	}
}
