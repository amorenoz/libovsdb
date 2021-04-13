package libovsdb

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"reflect"
	"strings"
	"sync"

	"github.com/cenkalti/rpc2"
	"github.com/cenkalti/rpc2/jsonrpc"
)

// OvsdbClient is an OVSDB client
type OvsdbClient struct {
	rpcClient     *rpc2.Client
	Schema        DatabaseSchema
	handlers      []NotificationHandler
	handlersMutex *sync.Mutex
	Cache         *TableCache
	stopCh        chan struct{}
}

func newOvsdbClient() *OvsdbClient {
	// Cache initialization is delayed because we first need to obtain the schema
	ovs := &OvsdbClient{
		handlersMutex: &sync.Mutex{},
		stopCh:        make(chan struct{}),
	}
	return ovs
}

// Constants defined for libovsdb
const (
	defaultTCPAddress  = "127.0.0.1:6640"
	defaultUnixAddress = "/var/run/openvswitch/ovnnb_db.sock"
	SSL                = "ssl"
	TCP                = "tcp"
	UNIX               = "unix"
)

// Connect to ovn, using endpoint in format ovsdb Connection Methods
// If address is empty, use default address for specified protocol
func Connect(endpoints string, database *DBModel, tlsConfig *tls.Config) (*OvsdbClient, error) {
	var c net.Conn
	var err error
	var u *url.URL

	for _, endpoint := range strings.Split(endpoints, ",") {
		if u, err = url.Parse(endpoint); err != nil {
			return nil, err
		}
		// u.Opaque contains the original endPoint with the leading protocol stripped
		// off. For example: endPoint is "tcp:127.0.0.1:6640" and u.Opaque is "127.0.0.1:6640"
		host := u.Opaque
		if len(host) == 0 {
			host = defaultTCPAddress
		}
		switch u.Scheme {
		case UNIX:
			path := u.Path
			if len(path) == 0 {
				path = defaultUnixAddress
			}
			c, err = net.Dial(u.Scheme, path)
		case TCP:
			c, err = net.Dial(u.Scheme, host)
		case SSL:
			c, err = tls.Dial("tcp", host, tlsConfig)
		default:
			err = fmt.Errorf("unknown network protocol %s", u.Scheme)
		}

		if err == nil {
			return newRPC2Client(c, database)
		}
	}

	return nil, fmt.Errorf("failed to connect to endpoints %q: %v", endpoints, err)
}

func newRPC2Client(conn net.Conn, database *DBModel) (*OvsdbClient, error) {
	ovs := newOvsdbClient()
	ovs.rpcClient = rpc2.NewClientWithCodec(jsonrpc.NewJSONCodec(conn))
	ovs.rpcClient.SetBlocking(true)
	ovs.rpcClient.Handle("echo", func(_ *rpc2.Client, args []interface{}, reply *[]interface{}) error {
		return ovs.echo(args, reply)
	})
	ovs.rpcClient.Handle("update", func(_ *rpc2.Client, args []interface{}, _ *[]interface{}) error {
		return ovs.update(args)
	})
	go ovs.rpcClient.Run()
	go ovs.handleDisconnectNotification()

	dbs, err := ovs.ListDbs()
	if err != nil {
		ovs.rpcClient.Close()
		return nil, err
	}

	found := false
	for _, db := range dbs {
		if db == database.Name() {
			found = true
			break
		}
	}
	if !found {
		ovs.rpcClient.Close()
		return nil, fmt.Errorf("target database not found")
	}

	schema, err := ovs.GetSchema(database.Name())
	if err == nil {
		ovs.Schema = *schema
		ovs.Cache = newTableCache(schema, database)
		ovs.Register(ovs.Cache)
	} else {
		ovs.rpcClient.Close()
		return nil, err
	}

	go ovs.Cache.Run(ovs.stopCh)

	return ovs, nil
}

// Register registers the supplied NotificationHandler to recieve OVSDB Notifications
func (ovs *OvsdbClient) Register(handler NotificationHandler) {
	ovs.handlersMutex.Lock()
	defer ovs.handlersMutex.Unlock()
	ovs.handlers = append(ovs.handlers, handler)
}

//Get Handler by index
func getHandlerIndex(handler NotificationHandler, handlers []NotificationHandler) (int, error) {
	for i, h := range handlers {
		if reflect.DeepEqual(h, handler) {
			return i, nil
		}
	}
	return -1, fmt.Errorf("handler not found")
}

// Unregister the supplied NotificationHandler to not recieve OVSDB Notifications anymore
func (ovs *OvsdbClient) Unregister(handler NotificationHandler) error {
	ovs.handlersMutex.Lock()
	defer ovs.handlersMutex.Unlock()
	i, err := getHandlerIndex(handler, ovs.handlers)
	if err != nil {
		return err
	}
	ovs.handlers = append(ovs.handlers[:i], ovs.handlers[i+1:]...)
	return nil
}

// NotificationHandler is the interface that must be implemented to receive notifcations
type NotificationHandler interface {
	// RFC 7047 section 4.1.6 Update Notification
	Update(context interface{}, tableUpdates TableUpdates)

	// RFC 7047 section 4.1.9 Locked Notification
	Locked([]interface{})

	// RFC 7047 section 4.1.10 Stolen Notification
	Stolen([]interface{})

	// RFC 7047 section 4.1.11 Echo Notification
	Echo([]interface{})

	Disconnected()
}

// RFC 7047 : Section 4.1.6 : Echo
func (ovs *OvsdbClient) echo(args []interface{}, reply *[]interface{}) error {
	*reply = args
	ovs.handlersMutex.Lock()
	defer ovs.handlersMutex.Unlock()
	for _, handler := range ovs.handlers {
		handler.Echo(nil)
	}
	return nil
}

// RFC 7047 : Update Notification Section 4.1.6
// Processing "params": [<json-value>, <table-updates>]
func (ovs *OvsdbClient) update(params []interface{}) error {
	if len(params) < 2 {
		return fmt.Errorf("invalid update message")
	}
	// Ignore params[0] as we dont use the <json-value> currently for comparison

	raw, ok := params[1].(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid update message")
	}
	var rowUpdates map[string]map[string]RowUpdate

	b, err := json.Marshal(raw)
	if err != nil {
		return err
	}
	err = json.Unmarshal(b, &rowUpdates)
	if err != nil {
		return err
	}

	// Update the local DB cache with the tableUpdates
	tableUpdates := getTableUpdatesFromRawUnmarshal(rowUpdates)
	ovs.handlersMutex.Lock()
	defer ovs.handlersMutex.Unlock()
	for _, handler := range ovs.handlers {
		handler.Update(params[0], tableUpdates)
	}

	return nil
}

// GetSchema returns the schema in use for the provided database name
// RFC 7047 : get_schema
func (ovs OvsdbClient) GetSchema(dbName string) (*DatabaseSchema, error) {
	args := NewGetSchemaArgs(dbName)
	var reply DatabaseSchema
	err := ovs.rpcClient.Call("get_schema", args, &reply)
	if err != nil {
		return nil, err
	}
	ovs.Schema = reply
	return &reply, err
}

// ListDbs returns the list of databases on the server
// RFC 7047 : list_dbs
func (ovs OvsdbClient) ListDbs() ([]string, error) {
	var dbs []string
	err := ovs.rpcClient.Call("list_dbs", nil, &dbs)
	if err != nil {
		return nil, fmt.Errorf("ListDbs failure - %v", err)
	}
	return dbs, err
}

// Transact performs the provided Operation's on the database
// RFC 7047 : transact
func (ovs OvsdbClient) Transact(operation ...Operation) ([]OperationResult, error) {
	var reply []OperationResult

	if ok := ovs.Schema.validateOperations(operation...); !ok {
		return nil, fmt.Errorf("validation failed for the operation")
	}

	args := NewTransactArgs(ovs.Schema.Name, operation...)
	err := ovs.rpcClient.Call("transact", args, &reply)
	if err != nil {
		return nil, err
	}
	return reply, nil
}

// MonitorAll is a convenience method to monitor every table/column
func (ovs OvsdbClient) MonitorAll(jsonContext interface{}) error {
	requests := make(map[string]MonitorRequest)
	for table, tableSchema := range ovs.Schema.Tables {
		var columns []string
		for column := range tableSchema.Columns {
			columns = append(columns, column)
		}
		requests[table] = MonitorRequest{
			Columns: columns,
			Select: MonitorSelect{
				Initial: true,
				Insert:  true,
				Delete:  true,
				Modify:  true,
			}}
	}
	return ovs.Monitor(jsonContext, requests)
}

// MonitorCancel will request cancel a previously issued monitor request
// RFC 7047 : monitor_cancel
func (ovs OvsdbClient) MonitorCancel(jsonContext interface{}) error {
	var reply OperationResult

	args := NewMonitorCancelArgs(jsonContext)

	err := ovs.rpcClient.Call("monitor_cancel", args, &reply)
	if err != nil {
		return err
	}
	if reply.Error != "" {
		return fmt.Errorf("Error while executing transaction: %s", reply.Error)
	}
	return nil
}

// Monitor will provide updates for a given table/column
// and populate the cache with them. Subsequent updates will be processed
// by the Update Notifications
// RFC 7047 : monitor
func (ovs OvsdbClient) Monitor(jsonContext interface{}, requests map[string]MonitorRequest) error {
	var reply TableUpdates

	args := NewMonitorArgs(ovs.Schema.Name, jsonContext, requests)

	// This totally sucks. Refer to golang JSON issue #6213
	var response map[string]map[string]RowUpdate
	err := ovs.rpcClient.Call("monitor", args, &response)
	reply = getTableUpdatesFromRawUnmarshal(response)
	if err != nil {
		return err
	}
	ovs.Cache.populate(reply)
	return nil
}

func getTableUpdatesFromRawUnmarshal(raw map[string]map[string]RowUpdate) TableUpdates {
	var tableUpdates TableUpdates
	tableUpdates.Updates = make(map[string]TableUpdate)
	for table, update := range raw {
		tableUpdate := TableUpdate{update}
		tableUpdates.Updates[table] = tableUpdate
	}
	return tableUpdates
}

func (ovs *OvsdbClient) clearConnection() {
	for _, handler := range ovs.handlers {
		if handler != nil {
			handler.Disconnected()
		}
	}
}

func (ovs *OvsdbClient) handleDisconnectNotification() {
	disconnected := ovs.rpcClient.DisconnectNotify()
	<-disconnected
	ovs.clearConnection()
}

// Disconnect will close the OVSDB connection
func (ovs OvsdbClient) Disconnect() {
	close(ovs.stopCh)
	ovs.rpcClient.Close()
}
