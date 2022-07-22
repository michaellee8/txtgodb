package datagodb

import (
	"context"
	dataparser "github.com/michaellee8/txtgodb/pkg/data/parser"
	"github.com/michaellee8/txtgodb/pkg/data/sinker"
	schemaparser "github.com/michaellee8/txtgodb/pkg/schema/parser"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"path/filepath"
	"sync"
)

const dataChBuffer = 100

type DataLoader struct {
	schemaParser schemaparser.SchemaParser
	dataParser   dataparser.DataParser
	dataSinker   sinker.DataSinker
}

func NewDataLoader(
	sp schemaparser.SchemaParser,
	dp dataparser.DataParser,
	ds sinker.DataSinker,
) *DataLoader {
	return &DataLoader{
		schemaParser: sp,
		dataParser:   dp,
		dataSinker:   ds,
	}
}

func (l *DataLoader) LoadData(
	specDir string,
	dataDir string,
	dbDriver string,
	dbDsn string,
	parallel bool,
) (err error) {
	const errMsg = "cannot load data"

	// Parallel read from data files will be used if parallel is used.
	// Parallel processing of multiple spec can be implemented for faster performance.

	specAbsDir, err := filepath.Abs(specDir)
	if err != nil {
		return errors.Wrap(err, errMsg)
	}

	dataAbsDir, err := filepath.Abs(dataDir)
	if err != nil {
		return errors.Wrap(err, errMsg)
	}

	specs, err := filepath.Glob(filepath.Join(specAbsDir, "*.csv"))
	if err != nil {
		return errors.Wrap(err, errMsg)
	}

	for _, spec := range specs {
		sch, err := l.schemaParser.Parse("file://" + spec)
		if err != nil {
			logrus.Error(errors.Wrap(err, "invalid spec skipped: "+spec))
			continue
		}
		specName := trimExt(filepath.Base(spec))
		dataFiles, err := filepath.Glob(filepath.Join(dataAbsDir, specName+"*.txt"))
		if err != nil {
			return errors.Wrap(err, errMsg)
		}
		dataCh := make(chan []any, dataChBuffer)
		// Error handling on the parser side can be done better here, should use context to cancel
		// pending reads. For now the goroutines will be automatically exit when the main program exits.
		go func() {
			defer close(dataCh)
			if parallel {
				var wg sync.WaitGroup
				for _, dataFile := range dataFiles {
					wg.Add(1)
					localDataFile := dataFile
					go func() {
						readCh, err := l.dataParser.Parse(sch, "file://"+localDataFile)
						if err != nil {
							logrus.Error(errors.Wrap(err, errMsg))
						}
						for r := range readCh {
							dataCh <- r
						}
						wg.Done()
					}()
				}
				wg.Wait()
			} else {
				for _, dataFile := range dataFiles {
					readCh, err := l.dataParser.Parse(sch, "file://"+dataFile)
					if err != nil {
						logrus.Error(errors.Wrap(err, errMsg))
						break
					}
					for r := range readCh {
						dataCh <- r
					}
				}
			}
		}()
		err = l.dataSinker.Sink(context.TODO(), sch, dbDriver, dbDsn, dataCh, specName)
		if err != nil {
			return errors.Wrap(err, errMsg)
		}
	}
	return nil
}

func trimExt(fileName string) string {
	return fileName[:len(fileName)-len(filepath.Ext(fileName))]
}
