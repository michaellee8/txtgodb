package sql

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/michaellee8/txtgodb/pkg/data/sinker"
	"github.com/michaellee8/txtgodb/pkg/schema"
	"github.com/pkg/errors"
	_ "modernc.org/sqlite"
	"strconv"
)

const driverPg = "postgres"
const driverMysql = "mysql"
const driverSqlite3 = "sqlite"

type SQLDataSinker struct {
}

func NewSQLDataSinker() *SQLDataSinker {
	return &SQLDataSinker{}
}

func (s *SQLDataSinker) Sink(sch schema.Schema, driver string, dsn string, dataCh <-chan []interface{}) (err error) {
	const errMsg = "cannot sink data into sql db"

	db, err := sql.Open(driver, dsn)
	if err != nil {
		return errors.Wrap(err, errMsg)
	}

	// A single-threaded implementation of sql sink is implemented here, to avoid race condition related problems,
	// a concurrent and batched solution can be implemented if a larger throughput is required and sequential
	// consistency is not required.

	_ = db
	return nil
}

func (_ *SQLDataSinker) getTableInitializeStatement(sch schema.Schema, driverName string, tableName string) (stmt string, err error) {
	const errMsg = "cannot generate table initialization statement"
	switch driverName {
	case driverPg:
	case driverMysql:
	case driverSqlite3:
	default:
		return "", errors.Wrap(errors.New("invalid driver"), errMsg)
	}
	stmt += `CREATE TABLE ` + tableName + ` ( `
	switch driverName {
	case driverPg:
		stmt += `id serial PRIMARY KEY, `
	case driverMysql:
		stmt += `id INT AUTO_INCREMENT PRIMARY KEY, `
	case driverSqlite3:
		stmt += `id INTEGER PRIMARY KEY, `
	}
	for _, field := range sch.Entries {
		switch field.DataType {
		case schema.DataTypeText:
			stmt += field.ColumnName + ` VARCHAR(` + strconv.Itoa(field.Width) + `) NOT NULL, `
		case schema.DataTypeBoolean:
			stmt += field.ColumnName + ` BOOLEAN NOT NULL, `
		case schema.DataTypeInteger:
			// It should be possible to determine the smallest integer type required here by calculate the field width,
			// so that storage space can be optimized.
			stmt += field.ColumnName + ` BIGINT NOT NULL, `
		}
	}

	// cut the last trailing comma
	stmt = stmt[:len(stmt)-2]

	stmt += `);`

	return stmt, nil
}

var _ sinker.DataSinker = (*SQLDataSinker)(nil)
