package gorm

import (
	"os"
	"testing"

	"github.com/acidlemon/aqua"
	_ "github.com/mattn/go-sqlite3"
)

func TestSuite(t *testing.T) {
	ts := aqua.NewTestSuite(t, "gorm")

	// show debug output of gorm
	os.Setenv("AQUA_DEBUG", "1")

	// disable gorm:update_time_stamp callback
	os.Setenv("AQUA_GORM_DISABLE_AUTO_TIMESTAMP", "1")

	ts.Run()
}
