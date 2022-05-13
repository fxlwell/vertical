package orm

import (
	"database/sql"
	"errors"
	vertical_conn "vertical/connector"
)

var (
	Err_node_unusable = errors.New("database node not usable")
)

func (g *God) query(query string, args ...interface{}) (*sql.Rows, error) {
	if db, err := vertical_conn.GetMysql(g.SlaveNode); err != nil {
		return nil, Err_node_unusable
	} else {
		return db.Query(query, args...)
	}
}

func (g *God) exec(query string, args ...interface{}) (sql.Result, error) {
	if db, err := vertical_conn.GetMysql(g.MasterNode); err != nil {
		return nil, Err_node_unusable
	} else {
		return db.Exec(query, args...)
	}
}
