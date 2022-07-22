package csv

import (
	"github.com/michaellee8/txtgodb/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
)

func TestCSVParser(t *testing.T) {
	parser := NewCSVParser()
	abspath, err := filepath.Abs("../../../testdata/specs/testformat1.csv")
	require.NoError(t, err)
	sch, err := parser.Parse("file://" + abspath)
	require.NoError(t, err)
	assert.Equal(t, schema.Schema{
		Entries: []schema.Entry{
			{
				ColumnName: "name",
				Width:      10,
				DataType:   schema.DataTypeText,
			},
			{
				ColumnName: "valid",
				Width:      1,
				DataType:   schema.DataTypeBoolean,
			},
			{
				ColumnName: "count",
				Width:      3,
				DataType:   schema.DataTypeInteger,
			},
		},
	}, sch)
}

func TestCSVParser2(t *testing.T) {
	parser := NewCSVParser()
	abspath, err := filepath.Abs("../../../testdata/specs/testformat2.csv")
	require.NoError(t, err)
	sch, err := parser.Parse("file://" + abspath)
	require.NoError(t, err)
	assert.Equal(t, schema.Schema{
		Entries: []schema.Entry{
			{ColumnName: "name", Width: 10, DataType: schema.DataTypeText},
			{ColumnName: "valid", Width: 1, DataType: schema.DataTypeBoolean},
			{ColumnName: "count", Width: 3, DataType: schema.DataTypeInteger},
			{ColumnName: "valid2", Width: 1, DataType: schema.DataTypeBoolean},
			{ColumnName: "count2", Width: 5, DataType: schema.DataTypeInteger},
			{ColumnName: "name3", Width: 5, DataType: schema.DataTypeText},
		},
	}, sch)
}

func TestCSVParserWithInvalidSchema(t *testing.T) {
	parser := NewCSVParser()
	abspath, err := filepath.Abs("../../../testdata/broken/brokenformat1.csv")
	require.NoError(t, err)
	_, err = parser.Parse("file://" + abspath)
	require.Error(t, err)
}
