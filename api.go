package libovsdb

import (
	"errors"
	"fmt"
	"reflect"
)

const (
	opInsert string = "insert"
)

// API defines basic operations to interact with the database
type API interface {
	// List populates a slice of Models objects based on their type
	// The function parameter must be a pointer to a slice of Models
	// If the slice is null, the entire cache will be copied into the slice
	// If it has a capacity != 0, only 'capacity' elements will be filled in
	List(result interface{}) error

	// Get retrieves a model from the cache
	// The way the object will be fetch depends on the data contained in the
	// provided model and the indexes defined in the associated schema
	// For more complex ways of searching for elements in the cache, the
	// preferred way is Where({condition}).List()
	Get(Model) error

	// Create returns the operation needed to add a model to the Database
	// Only non-default fields will be added to the transaction
	// If the _uuid field of the model has some content, it will be
	// treated as named-uuid
	Create(Model) (*Operation, error)

	// Create a ConditionalAPI based on the conditions provided
	// There are currently two supported conditions:
	//
	// 1. Fields Condition: which uses the data in model based on the schema indexes
	// (or user provided fields)
	//	e.g: Where (m Model, fields...interface{})
	//
	// 2. Predicate Condition:  which uses the provided callback to filter cached data
	//	e.g: Where (func(m Model)bool)
	//
	Where(arg interface{}, extra ...interface{}) ConditionalAPI
}

type ConditionalAPI interface {
	// List uses the condition to search on the cache and populates
	// the slice of Models objects based on their type
	List(result interface{}) error
}

// Error handling
// InputTypeError is used to report the user provided parameter has the wrong type
type InputTypeError struct {
	inputType reflect.Type
	reason    string
}

func (e *InputTypeError) Error() string {
	return fmt.Sprintf("Wrong parameter type (%s): %s", e.inputType.String(), e.reason)
}

// ConditionError is a wrapper around an error that is used to
// indicate the error occurred during condition creation
type ConditionError struct {
	err string
}

func (c ConditionError) Error() string {
	return fmt.Sprintf("Condition Error: %s", c.err)
}
func (c ConditionError) String() string {
	return c.Error()
}

// NotFoundError is used to inform the object or table was not found in the cache
var NotFoundError = errors.New("Object not found")

// api struct implements both API and ConditionalAPI
// Where() can be used to create a ConditionalAPI api
type api struct {
	cache *TableCache
	cond  conditionFactory
}

// List populates a slice of Models given as parameter based on the configured Condition
func (a api) List(result interface{}) error {
	resultPtr := reflect.ValueOf(result)
	if resultPtr.Type().Kind() != reflect.Ptr {
		return &InputTypeError{resultPtr.Type(), "Expected pointer to slice of valid Models"}
	}

	resultVal := reflect.Indirect(resultPtr)
	if resultVal.Type().Kind() != reflect.Slice {
		return &InputTypeError{resultPtr.Type(), "Expected pointer to slice of valid Models"}
	}

	table, err := a.getTableFromModel(reflect.New(resultVal.Type().Elem()).Interface())
	if err != nil {
		return err
	}

	if a.cond != nil && a.cond.table() != table {
		return &InputTypeError{resultPtr.Type(),
			fmt.Sprintf("Table derived from input type (%s) does not match Table from Condition (%s)", table, a.cond.table())}
	}

	tableCache := a.cache.Table(table)
	if tableCache == nil {
		return NotFoundError
	}

	// If given a null slice, fill it in the cache table completely, if not, just up to
	// its capability
	if resultVal.IsNil() {
		resultVal.Set(reflect.MakeSlice(resultVal.Type(), 0, tableCache.Len()))
	}
	i := resultVal.Len()

	for _, row := range tableCache.Rows() {
		elem := tableCache.Row(row)
		if i >= resultVal.Cap() {
			break
		}

		if a.cond != nil {
			if matches, err := a.cond.matches(elem); err != nil {
				return err
			} else if !matches {
				continue
			}
		}

		resultVal.Set(reflect.Append(resultVal, reflect.Indirect(reflect.ValueOf(elem))))
		i++
	}
	return nil
}

// Where returns a conditionalAPI based on the input parameters
func (a api) Where(first interface{}, extra ...interface{}) ConditionalAPI {
	var condition conditionFactory
	if first == nil {
		return errorApi{ConditionError{"Cannot create a condition with nil object"}}
	}

	if reflect.TypeOf(first).Kind() == reflect.Func {
		table, err := a.getTableFromFunc(first)
		if err != nil {
			return errorApi{ConditionError{err.Error()}}
		}

		condition, err = newPredicateCond(table, a.cache, first)
		if err != nil {
			return errorApi{ConditionError{err.Error()}}
		}

	} else {
		tableName, err := a.getTableFromModel(first)
		if tableName == "" {
			return errorApi{ConditionError{err.Error()}}
		}
		condition, err = newIndexCondition(a.cache.orm, tableName, first.(Model), extra...)
		if err != nil {
			return errorApi{ConditionError{err.Error()}}
		}
	}
	return newConditionalAPI(a.cache, condition)
}

// Get is a generic Get function capable of returning (through a provided pointer)
// a instance of any row in the cache.
// 'result' must be a pointer to an Model that exists in the DBModel
//
// The way the cache is search depends on the fields already populated in 'result'
// Any table index (including _uuid) will be used for comparison
func (a api) Get(model Model) error {
	table, err := a.getTableFromModel(model)
	if err != nil {
		return err
	}

	tableCache := a.cache.Table(table)
	if tableCache == nil {
		return NotFoundError
	}

	// If model contains _uuid value, we can access it via cache index
	ormInfo, err := newORMInfo(a.cache.orm.schema.Table(table), model)
	if err != nil {
		return err
	}
	if uuid, err := ormInfo.fieldByColumn("_uuid"); err != nil && uuid != nil {
		if found := tableCache.Row(uuid.(string)); found == nil {
			return NotFoundError
		} else {
			reflect.ValueOf(model).Elem().Set(reflect.Indirect(reflect.ValueOf(found)))
			return nil
		}
	}

	// Look across the entire cache for table index equality
	for _, row := range tableCache.Rows() {
		elem := tableCache.Row(row)
		equal, err := a.cache.orm.equalFields(table, model, elem.(Model))
		if err != nil {
			return err
		}
		if equal {
			reflect.ValueOf(model).Elem().Set(reflect.Indirect(reflect.ValueOf(elem)))
			return nil
		}
	}
	return NotFoundError
}

// Create is a generic function capable of creating any row in the DB
// A valud Model (pointer to object) must be provided.
func (a api) Create(model Model) (*Operation, error) {
	var namedUUID string
	var err error

	tableName, err := a.getTableFromModel(model)
	if err != nil {
		return nil, err
	}
	table := a.cache.orm.schema.Table(tableName)

	// Read _uuid field, and use it as named-uuid
	info, err := newORMInfo(table, model)
	if err != nil {
		return nil, err
	}
	if uuid, err := info.fieldByColumn("_uuid"); err == nil {
		namedUUID = uuid.(string)
	} else {
		return nil, err
	}

	row, err := a.cache.orm.newRow(tableName, model)
	if err != nil {
		return nil, err
	}

	insertOp := Operation{
		Op:       opInsert,
		Table:    tableName,
		Row:      row,
		UUIDName: namedUUID,
	}
	return &insertOp, nil
}

// getTableFromModel returns the table name from a Model object after performing
// type verifications on the model
func (a api) getTableFromModel(model interface{}) (string, error) {
	if _, ok := model.(Model); !ok {
		return "", &InputTypeError{reflect.TypeOf(model), "Type does not implement Model interface"}
	}

	if table := a.cache.dbModel.FindTable(reflect.TypeOf(model)); table == "" {
		return "", &InputTypeError{reflect.TypeOf(model), "Model not found in Database Model"}
	} else {
		return table, nil
	}
}

// getTableFromModel returns the table name from a the predicate after performing
// type verifications
func (a api) getTableFromFunc(predicate interface{}) (string, error) {
	predType := reflect.TypeOf(predicate)
	if predType.Kind() != reflect.Func {
		return "", &InputTypeError{predType, "Expected function"}
	}
	if predType.NumIn() != 1 || predType.NumOut() != 1 || predType.Out(0).Kind() != reflect.Bool {
		return "", &InputTypeError{predType, "Expected func(Model) bool"}
	}

	modelInterface := reflect.TypeOf((*Model)(nil)).Elem()
	modelType := predType.In(0)
	if !modelType.Implements(modelInterface) {
		return "", &InputTypeError{predType,
			fmt.Sprintf("Type %s does not implement Model interface", modelType.String())}
	}

	table := a.cache.dbModel.FindTable(modelType)
	if table == "" {
		return "", &InputTypeError{predType,
			fmt.Sprintf("Model %s not found in Database Model", modelType.String())}
	}
	return table, nil
}

// newAPI returns a new API to interact with the database
func newAPI(cache *TableCache) API {
	return api{
		cache: cache,
	}
}

// newConditionalAPI returns a new ConditionalAPI to interact with the database
func newConditionalAPI(cache *TableCache, cond conditionFactory) ConditionalAPI {
	return api{
		cache: cache,
		cond:  cond,
	}
}

// ErrorAPI is a ConditionalAPI that reports an error on every command
// It is used to delay the reporting of errors from API creation to method call
// That way the API is simplified
type errorApi struct {
	err error
}

// List returns the error
func (e errorApi) List(result interface{}) error {
	return e.err
}

// Where returns itself
func (e errorApi) Where(arg interface{}, extra ...interface{}) ConditionalAPI {
	return e
}
