package txt

import (
	"github.com/michaellee8/txtgodb/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
)

func TestTextDataParser_Parse(t *testing.T) {
	parser := NewTextDataParser()
	sch := schema.Schema{
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
	}
	fp, err := filepath.Abs("../../../testdata/data/testformat1_2021-07-06.txt")
	if err != nil {
		require.NoError(t, err)
	}
	url := "file://" + fp
	expectedRows := [][]interface{}{
		{
			"Diabetes",
			true,
			1,
		},
		{
			"Asthma",
			false,
			-14,
		},
		{
			"Stroke",
			true,
			122,
		},
	}
	var actualRows [][]interface{}
	ch, err := parser.Parse(sch, url)
	require.NoError(t, err)
	for row := range ch {
		actualRows = append(actualRows, row)
	}
	assert.Equal(t, expectedRows, actualRows)
}
