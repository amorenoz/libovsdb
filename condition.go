package libovsdb

import (
	"reflect"
)

// conditionFactory is an interface used by the API to generate conditions and match
// on cache objects
type conditionFactory interface {
	// matches returns true if a model matches the conditions
	matches(m Model) (bool, error)
	// returns the table that this condition generates conditions for
	table() string
}

// indexCond uses the information available in a model to generate conditions
// The conditions are based on the equality of the first available index.
// The priority of indexes is: {user_provided fields}, uuid, {schema index}
type indexCond struct {
	orm       *orm
	tableName string
	model     Model
	fields    []interface{}
}

func (c *indexCond) matches(m Model) (bool, error) {
	return c.orm.equalFields(c.tableName, c.model, m, c.fields...)
}

func (c *indexCond) table() string {
	return c.tableName
}

// newIndexCondition creates a new indexCond
func newIndexCondition(orm *orm, table string, model Model, fields ...interface{}) (conditionFactory, error) {
	return &indexCond{
		orm:       orm,
		tableName: table,
		model:     model,
		fields:    fields,
	}, nil
}

// predicateCond is a conditionFactory that calls a provided function pointer
// to match on models.
type predicateCond struct {
	tableName string
	predicate interface{}
}

// matches returns the result of the execution of the predicate
// Type verifications are not performed
func (c *predicateCond) matches(model Model) (bool, error) {
	ret := reflect.ValueOf(c.predicate).Call([]reflect.Value{reflect.ValueOf(model)})
	return ret[0].Bool(), nil
}

func (c *predicateCond) table() string {
	return c.tableName
}

// newIndexCondition creates a new predicateCond
func newPredicateCond(table string, predicate interface{}) (conditionFactory, error) {
	return &predicateCond{
		tableName: table,
		predicate: predicate,
	}, nil
}
