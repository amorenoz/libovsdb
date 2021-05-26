package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/ovn-org/libovsdb/ovsdb"
)

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of modelgen:\n")
	fmt.Fprintf(os.Stderr, "\tmodelgen [flags] OVS_SCHEMA\n")
	fmt.Fprintf(os.Stderr, "Flags:\n")
	flag.PrintDefaults()
}

var (
	outDirP  = flag.String("o", ".", "Directory where the generated files shall be stored")
	pkgNameP = flag.String("p", "ovsmodel", "Package name")
	dryRun   = flag.Bool("d", false, "Dry run")
	version  string
)

func writeFile(filename string, src []byte) error {
	if *dryRun {
		fmt.Printf("---- Content of file %s ----\n", filename)
		fmt.Print(string(src))
		fmt.Print("\n")
		return nil
	} else {
		return ioutil.WriteFile(filename, src, 0644)
	}
}

func main() {
	log.SetFlags(0)
	log.SetPrefix("modelgen: ")
	flag.Usage = usage
	flag.Parse()
	outDir := *outDirP
	pkgName := *pkgNameP

	/*Option handling*/
	outDir, err := filepath.Abs(outDir)
	if err != nil {
		log.Fatal(err)
	}

	if err := os.MkdirAll(outDir, 0755); err != nil {
		log.Fatal(err)
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

	var dbSchema ovsdb.DatabaseSchema
	if err := json.Unmarshal(schemaBytes, &dbSchema); err != nil {
		log.Fatal(err)
	}

	generators := []Generator{}
	for name, table := range dbSchema.Tables {
		generators = append(generators, NewTableGenerator(pkgName, name, &table))
	}
	generators = append(generators, NewDBModelGenerator(pkgName, &dbSchema))

	for _, gen := range generators {
		code, err := gen.Format()
		if err != nil {
			log.Fatal(err)
		}
		outFile := filepath.Join(outDir, gen.FileName())
		if err := writeFile(outFile, code); err != nil {
			log.Fatal(err)
		}
	}
}
