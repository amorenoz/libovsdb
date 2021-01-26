package libovsdb

import "encoding/json"
import "fmt"

// Operation represents an operation according to RFC7047 section 5.2
type Operation struct {
	Op        string                   `json:"op"`
	Table     string                   `json:"table"`
	Row       map[string]interface{}   `json:"row,omitempty"`
	Rows      []map[string]interface{} `json:"rows,omitempty"`
	Columns   []string                 `json:"columns,omitempty"`
	Mutations []interface{}            `json:"mutations,omitempty"`
	Timeout   int                      `json:"timeout,omitempty"`
	Where     []interface{}            `json:"where,omitempty"`
	Until     string                   `json:"until,omitempty"`
	UUIDName  string                   `json:"uuid-name,omitempty"`
}

// MarshalJSON marshalls 'Operation' to a byte array
// For 'select' operations, we dont omit the 'Where' field
// to allow selecting all rows of a table
func (o Operation) MarshalJSON() ([]byte, error) {
	type OpAlias Operation
	switch o.Op {
	case "select":
		where := o.Where
		if where == nil {
			where = make([]interface{}, 0, 0)
		}
		return json.Marshal(&struct {
			Where []interface{} `json:"where"`
			OpAlias
		}{
			Where:   where,
			OpAlias: (OpAlias)(o),
		})
	default:
		return json.Marshal(&struct {
			OpAlias
		}{
			OpAlias: (OpAlias)(o),
		})
	}
}

// MonitorRequests represents a group of monitor requests according to RFC7047
// We cannot use MonitorRequests by inlining the MonitorRequest Map structure till GoLang issue #6213 makes it.
// The only option is to go with raw map[string]interface{} option :-( that sucks !
// Refer to client.go : MonitorAll() function for more details
type MonitorRequests struct {
	Requests map[string]MonitorRequest `json:"requests,overflow"`
}

// MonitorRequest represents a monitor request according to RFC7047
type MonitorRequest struct {
	Columns []string      `json:"columns,omitempty"`
	Select  MonitorSelect `json:"select,omitempty"`
}

// MonitorSelect represents a monitor select according to RFC7047
type MonitorSelect struct {
	Initial bool `json:"initial,omitempty"`
	Insert  bool `json:"insert,omitempty"`
	Delete  bool `json:"delete,omitempty"`
	Modify  bool `json:"modify,omitempty"`
}

// TableUpdates is a collection of TableUpdate entries
// We cannot use TableUpdates directly by json encoding by inlining the TableUpdate Map
// structure till GoLang issue #6213 makes it.
// The only option is to go with raw map[string]map[string]interface{} option :-( that sucks !
// Refer to client.go : MonitorAll() function for more details
type TableUpdates struct {
	Updates map[string]TableUpdate `json:"updates,overflow"`
}

// TableUpdate represents a table update according to RFC7047
type TableUpdate struct {
	Rows map[string]RowUpdate `json:"rows,overflow"`
}

// NewTableUpdate creates a native table update from an OvsRowUpdate
// and a table schema
func NewTableUpdate(ovsUpdate map[string]OvsRowUpdate, table *TableSchema) (*TableUpdate, error) {
	update := TableUpdate{
		Rows: make(map[string]RowUpdate),
	}
	for key, ovsRowUpdate := range ovsUpdate {
		newNative, err := ovsRowUpdate.New.Data(table)
		if err != nil {
			return nil, err
		}
		oldNative, err := ovsRowUpdate.Old.Data(table)
		if err != nil {
			return nil, err
		}
		update.Rows[key] = RowUpdate{
			New: *newNative,
			Old: *oldNative,
		}
	}
	return &update, nil
}

// RowUpdate represents a row update without ovs-specific format
type RowUpdate struct {
	New Row
	Old Row
}

// OvsRowUpdate represents a row update according to RFC7047
type OvsRowUpdate struct {
	New OvsRow `json:"new,omitempty"`
	Old OvsRow `json:"old,omitempty"`
}

// OvsdbError is an OVS Error Condition
type OvsdbError struct {
	Error   string `json:"error"`
	Details string `json:"details,omitempty"`
}

// NewCondition creates a new condition as specified in RFC7047
func NewCondition(column *ColumnSchema, function string, value interface{}) ([]interface{}, error) {
	ovsVal, err := column.NativeToOvs(value)
	fmt.Printf("OVS VAL %v from  %v\n", ovsVal, value)
	if err != nil {
		return nil, err
	}
	return []interface{}{column.Name, function, ovsVal}, nil
}

// NewMutation creates a new mutation as specified in RFC7047
func NewMutation(column *ColumnSchema, mutator string, value interface{}) ([]interface{}, error) {
	ovsVal, err := column.NativeToOvs(value)
	if err != nil {
		return nil, err
	}
	return []interface{}{column.Name, mutator, ovsVal}, nil
}

// TransactResponse represents the response to a Transact Operation
type TransactResponse struct {
	Result []OperationResult `json:"result"`
	Error  string            `json:"error"`
}

// OperationResult is the result of an Operation
type OperationResult struct {
	Count   int            `json:"count,omitempty"`
	Error   string         `json:"error,omitempty"`
	Details string         `json:"details,omitempty"`
	UUID    UUID           `json:"uuid,omitempty"`
	Rows    []OvsResultRow `json:"rows,omitempty"`
}

func ovsSliceToGoNotation(val interface{}) (interface{}, error) {
	switch val.(type) {
	case []interface{}:
		sl := val.([]interface{})
		bsliced, err := json.Marshal(sl)
		if err != nil {
			return nil, err
		}

		switch sl[0] {
		case "uuid":
			var uuid UUID
			err = json.Unmarshal(bsliced, &uuid)
			return uuid, err
		case "set":
			var oSet OvsSet
			err = json.Unmarshal(bsliced, &oSet)
			return oSet, err
		case "map":
			var oMap OvsMap
			err = json.Unmarshal(bsliced, &oMap)
			return oMap, err
		}
		return val, nil
	}
	return val, nil
}

// TODO : add Condition, Function, Mutation and Mutator notations
