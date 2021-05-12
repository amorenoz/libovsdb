package client

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/ovn-org/libovsdb/ovsdb"
	"github.com/stretchr/testify/assert"
)

const (
	defOvsRunDir = "/var/run/openvswitch"
	defOvsSocket = "db.sock"
)

// ORMBridge is the simplified ORM model of the Bridge table
type bridgeType struct {
	UUID        string            `ovs:"_uuid"`
	Name        string            `ovs:"name"`
	OtherConfig map[string]string `ovs:"other_config"`
	ExternalIds map[string]string `ovs:"external_ids"`
	Ports       []string          `ovs:"ports"`
	Status      map[string]string `ovs:"status"`
}

// ORMovs is the simplified ORM model of the Bridge table
type ovsType struct {
	UUID    string   `ovs:"_uuid"`
	Bridges []string `ovs:"bridges"`
}

var defDB, _ = NewDBModel("Open_vSwitch", map[string]Model{
	"Open_vSwitch": &ovsType{},
	"Bridge":       &bridgeType{}})

var cfg *Config

func SetConfig() {
	cfg = &Config{}
	var ovsRunDir = os.Getenv("OVS_RUNDIR")
	if ovsRunDir == "" {
		ovsRunDir = defOvsRunDir
	}
	var ovsDb = os.Getenv("OVS_DB")
	if ovsDb == "" {
		cfg.Addr = "unix:" + ovsRunDir + "/" + defOvsSocket
	} else {
		cfg.Addr = ovsDb
	}
}

func TestConnectIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	SetConfig()
	if testing.Short() {
		t.Skip()
	}
	timeoutChan := make(chan bool)
	connected := make(chan bool)
	go func() {
		time.Sleep(10 * time.Second)
		timeoutChan <- true
	}()

	go func() {
		// Use Convenience params. Ignore failure even if any

		_, err := Connect(cfg.Addr, defDB, nil)
		if err != nil {
			log.Println("Couldnt establish OVSDB connection with Defult params. No big deal")
		}
	}()

	go func() {
		ovs, err := Connect(cfg.Addr, defDB, nil)
		if err != nil {
			connected <- false
		} else {
			connected <- true
			ovs.Disconnect()
		}
	}()

	select {
	case <-timeoutChan:
		t.Error("Connection Timed Out")
	case b := <-connected:
		if !b {
			t.Error("Couldnt connect to OVSDB Server")
		}
	}
}

func TestListDbsIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	SetConfig()
	if testing.Short() {
		t.Skip()
	}

	ovs, err := Connect(cfg.Addr, defDB, nil)
	if err != nil {
		t.Fatalf("Failed to Connect. error: %s", err)
	}
	reply, err := ovs.ListDbs()

	if err != nil {
		log.Fatal("ListDbs error:", err)
	}

	found := false
	for _, db := range reply {
		if db == "Open_vSwitch" {
			log.Println("Couldnt establish OVSDB connection with Defult params. No big deal")
			found = true
		}
	}

	if !found {
		t.Error("Expected: 'Open_vSwitch'", reply)
	}
	var b bytes.Buffer
	ovs.Schema.Print(&b)
	ovs.Disconnect()
}

func TestGetSchemasIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	SetConfig()
	if testing.Short() {
		t.Skip()
	}

	ovs, err := Connect(cfg.Addr, defDB, nil)
	if err != nil {
		t.Fatalf("Failed to Connect. error: %s", err)
	}

	dbName := "Open_vSwitch"
	reply, err := ovs.GetSchema(dbName)

	if err != nil {
		log.Fatal("GetSchemas error:", err)
		t.Error("Error Processing GetSchema for ", dbName, err)
	}

	if reply.Name != dbName {
		t.Error("Schema Name mismatch. Expected: ", dbName, "Got: ", reply.Name)
	}
	ovs.Disconnect()
}

var bridgeName = "gopher-br7"
var bridgeUUID string

func TestInsertTransactIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	SetConfig()

	ovs, err := Connect(cfg.Addr, defDB, nil)
	if err != nil {
		t.Fatalf("Failed to Connect. error: %s", err)
	}
	err = ovs.MonitorAll(nil)
	assert.Nil(t, err)

	// NamedUUID is used to add multiple related Operations in a single Transact operation
	namedUUID := "gopher"
	br := bridgeType{
		UUID: namedUUID,
		Name: bridgeName,
		ExternalIds: map[string]string{
			"go":     "awesome",
			"docker": "made-for-each-other",
		},
	}

	insertOp, err := ovs.API.Create(&br)
	assert.Nil(t, err)

	// Inserting a Bridge row in Bridge table requires mutating the open_vswitch table.
	ovsRow := ovsType{}
	mutateOp, err := ovs.API.Where(ovs.API.ConditionFromFunc(func(*ovsType) bool { return true })).
		Mutate(&ovsRow, []Mutation{
			{
				Field:   &ovsRow.Bridges,
				Mutator: ovsdb.MutateOperationInsert,
				Value:   []string{namedUUID},
			},
		})
	assert.Nil(t, err)

	operations := append([]ovsdb.Operation{*insertOp}, mutateOp...)
	reply, err := ovs.Transact(operations...)
	if err != nil {
		t.Fatal(err)
	}

	if len(reply) < len(operations) {
		t.Error("Number of Replies should be atleast equal to number of Operations")
	}
	ok := true
	for i, o := range reply {
		if o.Error != "" && i < len(operations) {
			t.Error("Transaction Failed due to an error :", o.Error, " details:", o.Details, " in ", operations[i])
			ok = false
		} else if o.Error != "" {
			t.Error("Transaction Failed due to an error :", o.Error)
			ok = false
		}
	}
	if ok {
		fmt.Println("Bridge Addition Successful : ", reply[0].UUID.GoUUID)
		bridgeUUID = reply[0].UUID.GoUUID
	}
	ovs.Disconnect()
}

func TestDeleteTransactIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	SetConfig()

	if bridgeUUID == "" {
		t.Skip()
	}

	ovs, err := Connect(cfg.Addr, defDB, nil)
	if err != nil {
		t.Fatalf("Failed to Connect. error: %s", err)
	}
	err = ovs.MonitorAll(nil)
	assert.Nil(t, err)

	deleteOp, err := ovs.API.Where(ovs.API.ConditionFromModel(&bridgeType{Name: bridgeName})).Delete()
	assert.Nil(t, err)

	ovsRow := ovsType{}
	mutateOp, err := ovs.API.Where(ovs.API.ConditionFromFunc(func(*ovsType) bool { return true })).
		Mutate(&ovsRow, []Mutation{
			{
				Field:   &ovsRow.Bridges,
				Mutator: ovsdb.MutateOperationDelete,
				Value:   []string{bridgeUUID},
			},
		})

	assert.Nil(t, err)

	operations := append(deleteOp, mutateOp...)
	reply, err := ovs.Transact(operations...)
	if err != nil {
		t.Fatal(err)
	}

	if len(reply) < len(operations) {
		t.Error("Number of Replies should be atleast equal to number of Operations")
	}
	ok := true
	for i, o := range reply {
		if o.Error != "" && i < len(operations) {
			t.Error("Transaction Failed due to an error :", o.Error, " in ", operations[i])
			ok = false
		} else if o.Error != "" {
			t.Error("Transaction Failed due to an error :", o.Error)
			ok = false
		}
	}
	if ok {
		fmt.Println("Bridge Delete Successful", reply[0].Count)
	}
	ovs.Disconnect()
}

func TestMonitorIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	SetConfig()
	if testing.Short() {
		t.Skip()
	}

	ovs, err := Connect(cfg.Addr, defDB, nil)
	if err != nil {
		t.Fatalf("Failed to Connect. error: %s", err)
	}

	err = ovs.MonitorAll(nil)
	if err != nil {
		t.Fatal(err)
	}
	ovs.Disconnect()
}

func TestNotifyIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	SetConfig()

	ovs, err := Connect(cfg.Addr, defDB, nil)
	if err != nil {
		t.Fatalf("Failed to Connect. error: %s", err)
	}

	notifyEchoChan := make(chan bool)

	notifier := Notifier{notifyEchoChan}
	ovs.Register(notifier)

	timeoutChan := make(chan bool)
	go func() {
		time.Sleep(10 * time.Second)
		timeoutChan <- true
	}()

	select {
	case <-timeoutChan:
		fmt.Println("Nothing changed to notify")
	case <-notifyEchoChan:
		break
	}
	ovs.Disconnect()
}

func TestRemoveNotifyIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	SetConfig()

	ovs, err := Connect(cfg.Addr, defDB, nil)
	if err != nil {
		t.Fatalf("Failed to Connect. error: %s", err)
	}

	notifyEchoChan := make(chan bool)

	notifier := Notifier{notifyEchoChan}
	ovs.Register(notifier)

	lenIni := len(ovs.handlers)
	_ = ovs.Unregister(notifier)
	lenEnd := len(ovs.handlers)

	if lenIni == lenEnd {
		log.Fatal("Failed to Unregister Notifier:")
	}

	ovs.Disconnect()
}

type Notifier struct {
	echoChan chan bool
}

func (n Notifier) Update(interface{}, ovsdb.TableUpdates) {
}
func (n Notifier) Locked([]interface{}) {
}
func (n Notifier) Stolen([]interface{}) {
}
func (n Notifier) Echo([]interface{}) {
	n.echoChan <- true
}
func (n Notifier) Disconnected() {
}

func TestTableSchemaValidationIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	SetConfig()

	ovs, err := Connect(cfg.Addr, defDB, nil)
	if err != nil {
		t.Fatalf("Failed to Connect. error: %s", err)
	}

	bridge := make(map[string]interface{})
	bridge["name"] = "docker-ovs"

	operation := ovsdb.Operation{
		Op:    "insert",
		Table: "InvalidTable",
		Row:   bridge,
	}
	_, err = ovs.Transact(operation)

	if err == nil {
		t.Error("Invalid Table Name Validation failed")
	}

	ovs.Disconnect()
}

func TestColumnSchemaInRowValidationIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	SetConfig()

	ovs, err := Connect(cfg.Addr, defDB, nil)
	if err != nil {
		t.Fatalf("Failed to Connect. error: %s", err)
	}

	bridge := make(map[string]interface{})
	bridge["name"] = "docker-ovs"
	bridge["invalid_column"] = "invalid_column"

	operation := ovsdb.Operation{
		Op:    "insert",
		Table: "Bridge",
		Row:   bridge,
	}

	_, err = ovs.Transact(operation)

	if err == nil {
		t.Error("Invalid Column Name Validation failed")
	}

	ovs.Disconnect()
}

func TestColumnSchemaInMultipleRowsValidationIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	SetConfig()

	ovs, err := Connect(cfg.Addr, defDB, nil)
	if err != nil {
		t.Fatalf("Failed to Connect. error: %s", err)
	}

	rows := make([]map[string]interface{}, 2)

	invalidBridge := make(map[string]interface{})
	invalidBridge["invalid_column"] = "invalid_column"

	bridge := make(map[string]interface{})
	bridge["name"] = "docker-ovs"

	rows[0] = invalidBridge
	rows[1] = bridge
	operation := ovsdb.Operation{
		Op:    "insert",
		Table: "Bridge",
		Rows:  rows,
	}
	_, err = ovs.Transact(operation)

	if err == nil {
		t.Error("Invalid Column Name Validation failed")
	}

	ovs.Disconnect()
}

func TestColumnSchemaValidationIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	SetConfig()
	if testing.Short() {
		t.Skip()
	}

	ovs, err := Connect(cfg.Addr, defDB, nil)
	if err != nil {
		t.Fatalf("Failed to Connect. error: %s", err)
	}

	operation := ovsdb.Operation{
		Op:      "select",
		Table:   "Bridge",
		Columns: []string{"name", "invalidColumn"},
	}
	_, err = ovs.Transact(operation)

	if err == nil {
		t.Error("Invalid Column Name Validation failed")
	}

	ovs.Disconnect()
}

func TestMonitorCancelIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	SetConfig()

	ovs, err := Connect(cfg.Addr, defDB, nil)
	if err != nil {
		t.Fatalf("Failed to Connect. error: %s", err)
	}

	monitorID := "f1b2ca48-aad7-11e7-abc4-cec278b6b50a"

	requests := make(map[string]ovsdb.MonitorRequest)
	requests["Bridge"] = ovsdb.MonitorRequest{
		Columns: []string{"name"},
		Select: ovsdb.MonitorSelect{
			Initial: true,
			Insert:  true,
			Delete:  true,
			Modify:  true,
		}}

	err = ovs.Monitor(monitorID, requests)
	if err != nil {
		t.Fatal(err)
	}

	err = ovs.MonitorCancel(monitorID)

	if err != nil {
		t.Error("MonitorCancel operation failed with error=", err)
	}
	ovs.Disconnect()
}
