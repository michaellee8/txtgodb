package sql

import (
	"context"
	"database/sql"
	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/elliotchance/pie/v2"
	"github.com/michaellee8/txtgodb/pkg/schema"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestSQLDataSinker_getTableInitializeStatement(t *testing.T) {
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
	s := NewSQLDataSinker()
	{
		stmt, err := s.getTableInitializeStatement(sch, driverMysql, "testtable")
		require.NoError(t, err)
		require.Equal(
			t,
			`CREATE TABLE testtable ( id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(10) NOT NULL, valid BOOLEAN NOT NULL, count BIGINT NOT NULL);`,
			stmt,
		)
	}
	{
		stmt, err := s.getTableInitializeStatement(sch, driverPg, "testtable")
		require.NoError(t, err)
		require.Equal(
			t,
			`CREATE TABLE testtable ( id serial PRIMARY KEY, name VARCHAR(10) NOT NULL, valid BOOLEAN NOT NULL, count BIGINT NOT NULL);`,
			stmt,
		)
	}
	{
		stmt, err := s.getTableInitializeStatement(sch, driverSqlite3, "testtable")
		require.NoError(t, err)
		require.Equal(
			t,
			`CREATE TABLE testtable ( id INTEGER PRIMARY KEY, name VARCHAR(10) NOT NULL, valid BOOLEAN NOT NULL, count BIGINT NOT NULL);`,
			stmt,
		)
	}
}

func TestSQLDataSinker_Sink(t *testing.T) {
	testDialect(t, driverSqlite3, "file:../../../testdata/tmp/test.db")
	defer func() {
		err := os.Remove("../../../testdata/tmp/test.db")
		require.NoError(t, err)
	}()

	// Only run these tests when you got the docker-compose file running
	// start test sql servers: docker-compose up
	// cleanup: docker-compose down -v

	//testDialect(t, driverPg, "postgres://postgres:very-secure-password@localhost:5432/txtgodb?sslmode=disable")
	//testDialect(t, driverMysql, "root:very-secure-password@tcp(localhost:3306)/txtgodb")
}

func testDialect(t *testing.T, driver string, dsn string) {
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
	rows := [][]interface{}{
		{"Diabetes", true, 1},
		{"Asthma", false, -14},
		{"Stroke", true, 122},
	}

	sinker := NewSQLDataSinker()

	dataCh := make(chan []any)
	go func() {
		for _, row := range rows {
			dataCh <- row
		}
		close(dataCh)
	}()

	tableName := "test_table"

	err := sinker.Sink(
		context.TODO(),
		sch,
		driver,
		dsn,
		dataCh,
		"test_table",
	)

	require.NoError(t, err)

	colNames := pie.Map(sch.Entries, func(entry schema.Entry) any {
		return entry.ColumnName
	})

	db, err := sql.Open(driver, dsn)

	require.NoError(t, err)

	goqudb := goqu.New(driver, db)

	for _, expectedRow := range rows {
		var whereExs []exp.Expression
		for i, colName := range colNames {
			whereExs = append(whereExs, goqu.C(colName.(string)).Eq(expectedRow[i]))
		}
		count, err := goqudb.From(tableName).Where(whereExs...).Count()
		require.NoError(t, err)
		require.Equal(t, int64(1), count)
	}

}
