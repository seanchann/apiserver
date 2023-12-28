package factory

import (
	"fmt"
	"strings"

	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/mysqls/mysql"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	_ "github.com/go-sql-driver/mysql"
	"k8s.io/klog"
	dbmysql "github.com/jinzhu/gorm"
)

//connectionStr: user:password@tcp(host:port)/dbname
func newMysqlClient(connectionStr string, debug bool) (*dbmysql.DB, error) {
	var err error
	connStr := string(connectionStr) + string("?parseTime=True")
	db, err := dbmysql.Open(string("mysql"), connStr)
	if err != nil && !strings.Contains(err.Error(), "Unknown database") {
		return nil, err
	}

	conInfo := strings.Split(connectionStr, "/")

	onlyConStr := conInfo[0] + "/"
	databaseName := conInfo[1]
	klog.V(5).Infof("database connection %v name %v err %v", onlyConStr, databaseName, err)
	if err != nil {
		tmpDB, err := dbmysql.Open(string("mysql"), onlyConStr)
		if err != nil {
			return nil, err
		}

		createDBSQL := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s DEFAULT CHARACTER SET = 'utf8' DEFAULT COLLATE 'utf8_general_ci'", databaseName)
		if err = tmpDB.Exec(createDBSQL).Error; err != nil {
			return nil, err
		}
		db, err = dbmysql.Open(string("mysql"), connStr)
		if err != nil && !strings.Contains(err.Error(), "Unknown database") {
			return nil, err
		}
	}
	if debug {
		db = db.Debug()
	}

	return db, db.DB().Ping()
}

func newMysqlStorage(c storagebackend.Config) (storage.Interface, DestroyFunc, error) {
	endpoints := c.Mysql.ServerList

	client, err := newMysqlClient(endpoints[0], c.Mysql.Debug)
	if err != nil {
		return nil, nil, err
	}

	destroyFunc := func() {
		client.Close()
	}

	return mysql.New(client, c.Codec, "v1", c.Mysql.ListDefaultLimit), destroyFunc, nil
}
