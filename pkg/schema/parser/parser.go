package parser

import (
	"github.com/michaellee8/txtgodb/pkg/schema"
)

type SchemaParser interface {
	Parse(url string) (sch schema.Schema, err error)
}
