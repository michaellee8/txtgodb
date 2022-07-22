package sinker

import (
	"context"
	"github.com/michaellee8/txtgodb/pkg/schema"
)

type DataSinker interface {
	Sink(
		ctx context.Context,
		sch schema.Schema,
		driver string,
		dsn string,
		dataCh <-chan []interface{},
		tableName string,
	) error
}
