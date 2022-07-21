package txt

import (
	"bufio"
	"github.com/michaellee8/txtgodb/pkg/data/parser"
	"github.com/michaellee8/txtgodb/pkg/schema"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io"
	"net/url"
	"os"
	"strconv"
)

type TextDataParser struct {
}

func NewTextDataParser() *TextDataParser {
	return &TextDataParser{}
}

func (p *TextDataParser) Parse(sch schema.Schema, urlStr string) (ch <-chan []interface{}, err error) {
	const errMsg = "cannot parse text data"

	fileURL, err := url.Parse(urlStr)
	if err != nil {
		return nil, errors.Wrap(err, errMsg)
	}

	file, err := os.Open(fileURL.Path)
	if err != nil {
		return nil, errors.Wrap(err, errMsg)
	}

	ownChan := make(chan []interface{})

	ch = ownChan

	go func() {
		defer func() {
			derr := file.Close()
			if derr != nil {
				logrus.Error(errors.Wrap(derr, errMsg))
			}
		}()

		defer close(ownChan)

		rdr := bufio.NewReader(file)

		for {
			isLastLine := false
			line, err := rdr.ReadString('\n')
			if errors.Is(err, io.EOF) {
				isLastLine = true
			} else if err != nil {
				logrus.Error(errors.Wrap(err, errMsg))
			} else {
				line = line[:len(line)-1]
			}
			var parsedRow []interface{}
			idx := 0
			// It is assumed that text fields must be seperated with other fields with at least one space or tab.
			// And integer fields must not be suffixed by a boolean field.
			// And integer fields cannot be suffixed by a text field that start with digits without a delimiter.
			// Otherwise, the parsing logic can be O( (num of characters) ^ (num of fields) ).
			// The whole line will be skipped if any invalid field is found.
			// It is also assumed that the data is ascii only, unicode support can be implemented with a similar algo
			// through.

			isInvalidLine := false
			for _, field := range sch.Entries {
				if idx >= len(line) {
					// Line has nothing to parse anymore.
					isInvalidLine = true
					break
				}
				for {
					if idx >= len(line) {
						isInvalidLine = true
						break
					}
					if line[idx] == ' ' || line[idx] == '\t' {
						idx++
					} else {
						break
					}
				}
				switch field.DataType {
				case schema.DataTypeBoolean:
					if line[idx] == '0' {
						idx += field.Width // Must be 1
						parsedRow = append(parsedRow, false)
						continue
					}
					if line[idx] == '1' {
						idx += field.Width // Must be 1
						parsedRow = append(parsedRow, true)
						continue
					}
					isInvalidLine = true
					break
				case schema.DataTypeInteger:
					seg := ""
					start := idx
					if idx < len(line) && idx-start < field.Width && line[idx] == '-' {
						seg += line[idx : idx+1]
						idx++
					}
					for {
						if idx < len(line) && idx-start < field.Width && '0' <= line[idx] && line[idx] <= '9' {
							seg += line[idx : idx+1]
							idx++
						} else {
							break
						}
					}
					v, err := strconv.Atoi(seg)
					if err != nil {
						logrus.Error(errors.Wrap(err, errMsg))
						isInvalidLine = true
						break
					}
					parsedRow = append(parsedRow, v)
					continue
				case schema.DataTypeText:
					seg := ""
					start := idx
					for {
						if idx < len(line) && idx-start < field.Width && (line[idx] != ' ' && line[idx] != '\t') {
							seg += line[idx : idx+1]
							idx++
						} else {
							break
						}
					}
					parsedRow = append(parsedRow, seg)
					continue
				}
			}
			if isInvalidLine {
				logrus.Debugf("got invalid line: %s", line)
			} else {
				ownChan <- parsedRow
			}
			if isLastLine {
				break
			}
		}
	}()

	return ch, err

}

var _ parser.DataParser = (*TextDataParser)(nil)
