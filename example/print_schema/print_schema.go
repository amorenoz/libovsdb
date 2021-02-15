package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"runtime/pprof"

	"github.com/ebay/libovsdb"
)

func usage() {
	fmt.Fprintf(os.Stderr, "Print schema information:\n")
	fmt.Fprintf(os.Stderr, "\tprint_schemas [flags] OVS_SCHEMA\n")
	fmt.Fprintf(os.Stderr, "Flag:\n")
	flag.PrintDefaults()
}

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to this file")
var memprofile = flag.String("memoryprofile", "", "write memory profile to this file")
var ntimes = flag.Int("ntimes", 1, "Parse the schema N times. Useful for profiling")

var schemas []libovsdb.DatabaseSchema

func main() {
	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if len(flag.Args()) != 1 {
		flag.Usage()
		os.Exit(2)
	}

	schemaFile, err := os.Open(flag.Args()[0])
	if err != nil {
		log.Fatal(err)
	}
	defer schemaFile.Close()

	schemaBytes, err := ioutil.ReadAll(schemaFile)
	if err != nil {
		log.Fatal(err)
	}

	schemas = make([]libovsdb.DatabaseSchema, *ntimes)
	for i := 0; i < *ntimes; i++ {
		if err := json.Unmarshal(schemaBytes, &schemas[i]); err != nil {
			log.Fatal(err)
		}
	}
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
	for i := 0; i < *ntimes; i++ {
		schemas[i].Print(os.Stdout)
	}
}
