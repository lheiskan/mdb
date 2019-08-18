package mdb

import (
	"os"

	"github.com/bmatsuo/lmdb-go/lmdb"
)

type Config struct {
	EnvFlags uint
	SizeMbs  int64
	Mode     os.FileMode
	MaxDBs   int
	Readonly bool
}

func NewConfig() *Config {

	return &Config{
		EnvFlags: lmdb.NoSync,
		SizeMbs:  1024,
		Mode:     0644,
		MaxDBs:   5,
		Readonly: false,
	}

}
