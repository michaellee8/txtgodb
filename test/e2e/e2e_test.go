package e2e

import (
	"github.com/michaellee8/txtgodb/pkg/datagodb"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDataLoader(t *testing.T) {
	loader := datagodb.InitializeDataLoader()
	err := loader.LoadData(
		"../../pkg/testdata/specs",
		"../../pkg/testdata/data",
		"sqlite",
		"file:../../pkg/testdata/tmp/e2e_test.db",
		false,
	)
	require.NoError(t, err)

	err = loader.LoadData(
		"../../pkg/testdata/specs",
		"../../pkg/testdata/data",
		"sqlite",
		"file:../../pkg/testdata/tmp/e2e_test_parallel.db",
		true,
	)
	require.NoError(t, err)
}
