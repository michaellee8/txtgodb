package main

import (
	"flag"
	"github.com/michaellee8/txtgodb/pkg/datagodb"
	"github.com/sirupsen/logrus"
)

var (
	spec     string
	data     string
	driver   string
	dsn      string
	parallel bool
)

func main() {
	flag.StringVar(&spec, "spec", "", "directory containing specification files")
	flag.StringVar(&data, "data", "", "directory containing data files")
	flag.StringVar(&driver, "driver", "", "name of sql driver, can be mysql, postgres, sqlite")
	flag.StringVar(&dsn, "dsn", "", "dsn for the sql database")
	flag.BoolVar(&parallel, "parallel", false, "enable parallel data ingestion")

	flag.Parse()

	if spec == "" || data == "" || driver == "" || dsn == "" {
		flag.PrintDefaults()
		return
	}

	loader := datagodb.InitializeDataLoader()

	err := loader.LoadData(
		spec,
		data,
		driver,
		dsn,
		parallel,
	)

	if err != nil {
		logrus.Fatal(err)
		return
	}
}
