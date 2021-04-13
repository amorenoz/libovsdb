package main

import (
	"fmt"
	"log"

	"github.com/ovn-org/libovsdb"
)

// Silly game that detects creation of Bridge named "stop" and exits
// Just a demonstration of how an app can use libovsdb library to configure and manage OVS
const (
	bridgeTable = "Bridge"
	ovsTable    = "Open_vSwitch"
)

// ORMBridge is the simplified ORM model of the Bridge table
type ormBridge struct {
	UUID        string            `ovs:"_uuid"`
	Name        string            `ovs:"name"`
	OtherConfig map[string]string `ovs:"other_config"`
	ExternalIds map[string]string `ovs:"external_ids"`
	Ports       []string          `ovs:"ports"`
	Status      map[string]string `ovs:"status"`
}

// Table returns the table name
func (b *ormBridge) Table() string {
	return bridgeTable
}

// ORMOVS is the simplified ORM model of the Open_vSwitch table
type ormOvs struct {
	UUID string `ovs:"_uuid"`
}

// Table returns the table name
func (b *ormOvs) Table() string {
	return ovsTable
}

var quit chan bool
var update chan libovsdb.Model

var rootUUID string

func play(ovs *libovsdb.OvsdbClient) {
	go processInput(ovs)
	for model := range update {
		if model.Table() == bridgeTable {
			bridge := model.(*ormBridge)
			if bridge.Name == "stop" {
				fmt.Printf("Bridge stop detected: %+v\n", *bridge)
				ovs.Disconnect()
				quit <- true
			} else {
				fmt.Printf("Current list of bridges:\n")
				var bridges []ormBridge
				if err := ovs.API.List(&bridges); err != nil {
					log.Fatal(err)
				}
				for _, b := range bridges {
					fmt.Printf("UUID: %s  Name: %s\n", b.UUID, b.Name)
				}
			}
		}
	}
}

func createBridge(ovs *libovsdb.OvsdbClient, bridgeName string) {
	namedUUID := "gopher"
	// bridge row to insert
	bridge := make(map[string]interface{})
	bridge["name"] = bridgeName

	// simple insert operation
	insertOp := libovsdb.Operation{
		Op:       "insert",
		Table:    bridgeTable,
		Row:      bridge,
		UUIDName: namedUUID,
	}

	uuidParameter := libovsdb.UUID{GoUUID: rootUUID}
	mutateUUID := []libovsdb.UUID{{GoUUID: namedUUID}}
	mutateSet, _ := libovsdb.NewOvsSet(mutateUUID)
	mutation := libovsdb.NewMutation("bridges", "insert", mutateSet)
	condition := libovsdb.NewCondition("_uuid", "==", uuidParameter)

	// simple mutate operation
	mutateOp := libovsdb.Operation{
		Op:        "mutate",
		Table:     ovsTable,
		Mutations: []interface{}{mutation},
		Where:     []interface{}{condition},
	}

	operations := []libovsdb.Operation{insertOp, mutateOp}
	reply, err := ovs.Transact(operations...)
	if err != nil {
		log.Fatal(err)
	}

	if len(reply) < len(operations) {
		fmt.Println("Number of Replies should be atleast equal to number of Operations")
	}
	ok := true
	for i, o := range reply {
		if o.Error != "" && i < len(operations) {
			fmt.Println("Transaction Failed due to an error :", o.Error, " details:", o.Details, " in ", operations[i])
			ok = false
		} else if o.Error != "" {
			fmt.Println("Transaction Failed due to an error :", o.Error)
			ok = false
		}
	}
	if ok {
		fmt.Println("Bridge Addition Successful : ", reply[0].UUID.GoUUID)
	}
}

func processInput(ovs *libovsdb.OvsdbClient) {
	for {
		fmt.Printf("\n Enter a Bridge Name : ")
		var bridgeName string
		fmt.Scanf("%s", &bridgeName)
		createBridge(ovs, bridgeName)
	}
}

func main() {
	quit = make(chan bool)
	update = make(chan libovsdb.Model)

	dbmodel, err := libovsdb.NewDBModel("Open_vSwitch", []libovsdb.Model{&ormBridge{}, &ormOvs{}})
	if err != nil {
		log.Fatal("Unable to create DB model ", err)
	}
	// By default libovsdb connects to 127.0.0.0:6400.
	ovs, err := libovsdb.Connect("tcp:", dbmodel, nil)

	// If you prefer to connect to OVS in a specific location :
	// ovs, err := libovsdb.Connect("tcp:192.168.56.101:6640", nil)

	if err != nil {
		log.Fatal("Unable to Connect ", err)
	}

	ovs.Cache.AddEventHandler(&libovsdb.EventHandlerFuncs{
		AddFunc: func(table string, model libovsdb.Model) {
			if table == bridgeTable {
				update <- model
			}
		},
	})

	err = ovs.MonitorAll("")
	if err != nil {
		log.Fatal(err)
	}

	rootUUID = ovs.Cache.Table(ovsTable).Rows()[0]

	fmt.Println(`Silly game of stopping this app when a Bridge with name "stop" is monitored !`)
	go play(ovs)
	<-quit
}
