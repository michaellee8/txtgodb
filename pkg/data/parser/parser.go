package parser

import (
	"github.com/michaellee8/txtgodb/pkg/schema"
)

type DataParser interface {
	Parse(sch schema.Schema, url string) (<-chan []interface{}, error)
}
