package sinker

import "github.com/michaellee8/txtgodb/pkg/schema"

type DataSinker interface {
	Sink(sch schema.Schema, driver string, dsn string, dataCh <-chan []interface{}) error
}
