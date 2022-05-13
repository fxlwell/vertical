package connector

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	vertical_log "vertical/log"
	vertical_util "vertical/util"
	"time"
)

type MysqlConf struct {
	Addr     string
	Username string
	Password string
	Database string

	MaxIdle     int
	MaxOpen     int
	MaxLifeTime time.Duration
}

var mysqlConnMapping *vertical_util.CMap = vertical_util.NewCMap()

func SetupMysql(configs map[string]MysqlConf) error {
	for sn, config := range configs {
		db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4",
			config.Username, config.Password, config.Addr, config.Database))
		if err != nil {
			vertical_log.Fatalf("mysql cluster [%s: %s@%s/%s] setup failed: %s",
				sn, config.Username, config.Addr, config.Database)
			return fmt.Errorf("open failed[%s]: %s@%s/%s", sn, config.Username, config.Addr, config.Database)
		}
		if err = db.Ping(); err != nil {
			vertical_log.Fatalf("mysql cluster [%s: %s@%s/%s] ping failed when setup: %s",
				sn, config.Username, config.Addr, config.Database)
			return fmt.Errorf("ping failed[%s]: %s@%s/%s", sn, config.Username, config.Addr, config.Database)
		}
		db.SetConnMaxLifetime(config.MaxLifeTime)
		db.SetMaxIdleConns(config.MaxIdle)
		db.SetMaxOpenConns(config.MaxOpen)
		mysqlConnMapping.Set(sn, db)
	}

	return nil
}

func GetMysql(sn string) (*sql.DB, error) {
	if conn, exists := mysqlConnMapping.Get(sn); exists {
		return conn.(*sql.DB), nil
	}
	vertical_log.Warnf("get mysql cluster[%s], but not ready", sn)
	return nil, fmt.Errorf("have no mysql cluster: %s", sn)
}

func MustGetMysql(sn string) *sql.DB {
	conn, err := GetMysql(sn)
	if err != nil {
		panic(err)
	}
	return conn
}
