package schema

type DataType int

const (
	DataTypeText DataType = iota + 1
	DataTypeBoolean
	DataTypeInteger
)

type Entry struct {
	ColumnName string
	Width      int
	DataType   DataType
}

type Schema struct {
	Entries []Entry
}
