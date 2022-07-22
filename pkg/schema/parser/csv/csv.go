package csv

import (
	"encoding/csv"
	"github.com/google/wire"
	"github.com/michaellee8/txtgodb/pkg/schema"
	"github.com/michaellee8/txtgodb/pkg/schema/parser"
	"github.com/pkg/errors"
	"io"
	"net/url"
	"os"
	"strconv"
)

//goland:noinspection GoNameStartsWithPackageName
type CSVSchemaParser struct {
}

func NewCSVParser() *CSVSchemaParser {
	return &CSVSchemaParser{}
}

func (p *CSVSchemaParser) Parse(urlStr string) (sch schema.Schema, err error) {
	const errMsg = "cannot parse csv schema"

	fileUrl, err := url.Parse(urlStr)

	if err != nil {
		return sch, errors.Wrap(err, errMsg)
	}

	if fileUrl.Scheme != "file" {
		return sch, errors.Wrap(errors.New("invalid file URL"), errMsg)
	}

	file, err := os.Open(fileUrl.Path)

	if err != nil {
		return sch, errors.Wrap(err, errMsg)
	}

	defer func() {
		derr := file.Close()
		if err == nil && derr != nil {
			err = errors.Wrap(derr, errMsg)
		}
	}()

	rdr := csv.NewReader(file)

	// Check if first line conforms to spec
	fl, err := rdr.Read()
	if err != nil {
		return sch, errors.Wrap(err, errMsg)
	}
	if len(fl) != 3 {
		return sch, errors.Wrap(errors.New("incorrect number of fields"), errMsg)
	}

	for {
		line, err := rdr.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return sch, errors.Wrap(err, errMsg)
		}
		if len(line) != 3 {
			return sch, errors.Wrap(errors.New("incorrect number of fields"), errMsg)
		}
		colName := line[0]
		width, err := strconv.Atoi(line[1])
		if err != nil {
			return sch, errors.Wrap(err, errMsg)
		}
		var dataType schema.DataType
		switch line[2] {
		case "TEXT":
			dataType = schema.DataTypeText
		case "BOOLEAN":
			dataType = schema.DataTypeBoolean
			if width != 1 {
				return sch, errors.Wrap(errors.New("invalid length for BOOLEAN field"), errMsg)
			}
		case "INTEGER":
			dataType = schema.DataTypeInteger
		default:
			return sch, errors.Wrap(errors.New("invalid dataType"), errMsg)
		}
		sch.Entries = append(sch.Entries, schema.Entry{
			ColumnName: colName,
			Width:      width,
			DataType:   dataType,
		})
	}
	return sch, err
}

var _ parser.SchemaParser = (*CSVSchemaParser)(nil)

var ParserSet = wire.NewSet(
	wire.Bind(new(parser.SchemaParser), new(*CSVSchemaParser)),
	NewCSVParser,
)
