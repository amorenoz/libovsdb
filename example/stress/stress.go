package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"

	"github.com/ovn-org/libovsdb"
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
	return "Bridge"
}

// ORMovs is the simplified ORM model of the Bridge table
type ormOvs struct {
	UUID    string   `ovs:"_uuid"`
	Bridges []string `ovs:"bridges"`
}

// Table returns the table name
func (b *ormOvs) Table() string {
	return "Open_vSwitch"
}

var (
	cpuprofile = flag.String("cpuprofile", "", "write cpu profile to this file")
	memprofile = flag.String("memoryprofile", "", "write memory profile to this file")
	nins       = flag.Int("ninserts", 100, "insert this number of elements in the database")
	verbose    = flag.Bool("verbose", false, "Be verbose")
	connection = flag.String("ovsdb", "unix:/var/run/openvswitch/db.sock", "OVSDB connection string")
	dbModel    *libovsdb.DBModel

	rootUUID   string
	insertions int
	deletions  int
)

func run() {
	ovs, err := libovsdb.Connect(*connection, dbModel, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer ovs.Disconnect()
	ovs.Cache.AddEventHandler(
		&libovsdb.EventHandlerFuncs{
			AddFunc: func(table string, model libovsdb.Model) {
				insertions++
			},
			DeleteFunc: func(table string, model libovsdb.Model) {
				deletions++
			},
		},
	)

	if err := ovs.MonitorAll(""); err != nil {
		log.Fatal(err)
	}

	// Get root UUID
	for _, uuid := range ovs.Cache.Table("Open_vSwitch").Rows() {
		rootUUID = uuid
		if *verbose {
			fmt.Printf("rootUUID is %v", rootUUID)
		}
	}

	// Remove all existing bridges
	var bridges []ormBridge
	if err := ovs.API.List(&bridges); err != nil {
		log.Fatal(err)
	}
	for _, bridge := range bridges {
		deleteBridge(ovs, &bridge)
	}

	for i := 0; i < *nins; i++ {
		createBridge(ovs, i)
	}
}

func transact(ovs *libovsdb.OvsdbClient, operations []libovsdb.Operation) (ok bool, uuid string) {
	reply, _ := ovs.Transact(operations...)

	if len(reply) < len(operations) {
		fmt.Println("Number of Replies should be atleast equal to number of Operations")
	}
	ok = true
	for i, o := range reply {
		if o.Error != "" && i < len(operations) {
			fmt.Println("Transaction Failed due to an error :", o.Error, " details:", o.Details, " in ", operations[i])
			ok = false
		} else if o.Error != "" {
			fmt.Println("Transaction Failed due to an error :", o.Error)
			ok = false
		}
	}
	uuid = reply[0].UUID.GoUUID
	return
}

func deleteBridge(ovs *libovsdb.OvsdbClient, bridge *ormBridge) {
	deleteOp, err := ovs.API.Where(bridge).Delete()
	if err != nil {
		log.Fatal(err)
	}
	ovsRow := ormOvs{
		UUID: rootUUID,
	}

	mutateOp, err := ovs.API.Where(&ovsRow).Mutate(&ovsRow, []libovsdb.Mutation{
		{
			Field:   &ovsRow.Bridges,
			Mutator: "delete",
			Value:   []string{bridge.UUID},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	operations := append(deleteOp, mutateOp...)
	ok, _ := transact(ovs, operations)
	if ok {
		if *verbose {
			fmt.Println("Bridge Deletion Successful : ", bridge.UUID)
		}
	}
}

func createBridge(ovs *libovsdb.OvsdbClient, iter int) {
	bridge := ormBridge{
		UUID: "gopher",
		Name: fmt.Sprintf("bridge-%d", iter),
		OtherConfig: map[string]string{
			"foo":  "bar",
			"fake": "config",
		},
		ExternalIds: map[string]string{
			"key1": "val1",
			"key2": "val2",
		},
	}
	insertOp, err := ovs.API.Create(&bridge)
	if err != nil {
		log.Fatal(err)
	}
	ovsRow := ormOvs{}
	mutateOp, err := ovs.API.Where(&ormOvs{UUID: rootUUID}).Mutate(&ovsRow, []libovsdb.Mutation{
		{
			Field:   &ovsRow.Bridges,
			Mutator: "insert",
			Value:   []string{bridge.UUID},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	operations := []libovsdb.Operation{*insertOp, mutateOp[0]}
	ok, uuid := transact(ovs, operations)
	if ok {
		if *verbose {
			fmt.Println("Bridge Addition Successful : ", uuid)
		}
	}
}
func main() {
	flag.Parse()
	var err error
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal(err)
		}
		defer pprof.StopCPUProfile()
	}

	dbModel, err = libovsdb.NewDBModel("Open_vSwitch", []libovsdb.Model{&ormBridge{}, &ormOvs{}})
	if err != nil {
		log.Fatal(err)
	}

	run()

	fmt.Printf("Summary:\n")
	fmt.Printf("\tInsertions: %d\n", insertions)
	fmt.Printf("\tDeletions: %d\n", deletions)

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
}
