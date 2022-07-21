package txt

import (
	"github.com/michaellee8/txtgodb/pkg/schema"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
)

func TestTextDataParser_Parse(t *testing.T) {
	testParse(
		t,
		schema.Schema{
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
		},
		"testformat1_2021-07-06.txt",
		[][]interface{}{
			{"Diabetes", true, 1},
			{"Asthma", false, -14},
			{"Stroke", true, 122},
			{"Stroke", true, 122},
		},
	)

	testParse(
		t,
		schema.Schema{
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
		},
		"testformat1_2021-07-07.txt",
		[][]interface{}{
			{"Strokertgh", true, 122},
			{"dsdadjaod0", true, 212},
			{"dadjaidjoa", false, 129},
			{"d", true, 333},
			{"dddddaaaaa", false, 123},
		},
	)
}

func testParse(t *testing.T, sch schema.Schema, filename string, expectedRows [][]interface{}) {

	parser := NewTextDataParser()
	fp, err := filepath.Abs("../../../testdata/data/" + filename)
	if err != nil {
		require.NoError(t, err)
	}
	url := "file://" + fp
	var actualRows [][]interface{}
	ch, err := parser.Parse(sch, url)
	require.NoError(t, err)
	for row := range ch {
		actualRows = append(actualRows, row)
	}
	require.Equal(t, expectedRows, actualRows)
}
