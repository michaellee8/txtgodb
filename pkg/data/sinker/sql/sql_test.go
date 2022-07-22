package sql

import (
	"github.com/michaellee8/txtgodb/pkg/schema"
	"github.com/stretchr/testify/require"
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
