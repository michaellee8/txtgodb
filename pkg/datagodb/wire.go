//go:build wireinject
// +build wireinject

package datagodb

import (
	"github.com/google/wire"
	"github.com/michaellee8/txtgodb/pkg/data/parser/txt"
	"github.com/michaellee8/txtgodb/pkg/data/sinker/sql"
	"github.com/michaellee8/txtgodb/pkg/schema/parser/csv"
)

func InitializeDataLoader() *DataLoader {
	wire.Build(csv.ParserSet, txt.ParserSet, sql.SinkerSet, NewDataLoader)
	return nil
}
