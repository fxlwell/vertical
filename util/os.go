package util

import (
	"fmt"
	"os"
)

var (
	Hostname string
)

func init() {
	Hostname, _ = os.Hostname()
}

func OpenOrCreateDir(path string, mode os.FileMode) (*os.File, error) {
	if dp, err := os.Open(path); err == nil {
		return dp, nil
	} else if os.IsNotExist(err) {
		if err := os.MkdirAll(path, mode); err != nil {
			return nil, err
		}
		return os.Open(path)
	} else {
		return nil, err
	}
}

func MustMkdirAll(path string, mode os.FileMode) {
	if fi, err := os.Stat(path); err != nil && !os.IsNotExist(err) {
		panic(err)
	} else if os.IsNotExist(err) {
		if err := os.MkdirAll(path, 0755); err != nil {
			panic(err)
		}
	} else if !fi.Mode().IsDir() {
		panic(fmt.Sprintf("path[%s] already exists and is not directory", path))
	}
}
