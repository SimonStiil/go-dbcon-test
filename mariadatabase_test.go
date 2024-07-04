package main

import (
	"database/sql"
	"testing"
)

type MariaDBTest struct {
	DB     *MariaDatabase
	Config ConfigType
}

func Test_Maria_DB(t *testing.T) {
	dbt := new(MariaDBTest)
	setupTestlogging()
	ConfigRead("config", &dbt.Config)
	dbt.DB = &MariaDatabase{
		Config: &dbt.Config.Mysql,
	}
	t.Run("initialize db", func(t *testing.T) {
		dbt.DB.Init()
	})
	tableName := dbt.DB.RandStringRunes(10)
	testKey := dbt.DB.RandStringRunes(10)
	testValue := dbt.DB.RandStringRunes(30)
	var conn *sql.DB
	t.Run("Create Database Connection", func(t *testing.T) {
		var err error
		conn, err = dbt.DB.CreateConnection()
		if err != nil {
			t.Errorf("Error connecting to database: %+v", err)
			return
		}
	})
	t.Run("Create Table", func(t *testing.T) {
		err := dbt.DB.CreateNamespace(conn, tableName)
		if err != nil {
			t.Errorf("Error Creating table: %+v", err)
		}
	})
	t.Run("Create Key", func(t *testing.T) {
		err := dbt.DB.Set(conn, tableName, testKey, testValue)
		if err != nil {
			t.Errorf("Error Setting row: %+v", err)
		}
	})
	t.Run("Getting Key", func(t *testing.T) {
		value, err := dbt.DB.Get(conn, tableName, testKey)
		if err != nil {
			t.Errorf("Error Getting row: %+v", err)
		}
		if value != testValue {
			t.Errorf("Error Values not matching for row: %v != %v", value, testValue)
		}
	})
	t.Run("Delete Key", func(t *testing.T) {
		err := dbt.DB.DeleteKey(conn, tableName, testKey)
		if err != nil {
			t.Errorf("Error Deleting row: %+v", err)
		}
	})
	t.Run("Delete Table", func(t *testing.T) {
		err := dbt.DB.DeleteNamespace(conn, tableName)
		if err != nil {
			t.Errorf("Error Deleting table: %+v", err)
		}
	})
	conn.Close()
}

func Test_Integration_Maria_DB(t *testing.T) {
	dbt := new(MariaDBTest)
	setupTestlogging()
	ConfigRead("config", &dbt.Config)
	dbt.DB = &MariaDatabase{
		Config: &dbt.Config.Mysql,
	}
	dbt.DB.Init()
	result := dbt.DB.ConnectionTest()
	if !result.ok() {
		t.Errorf("Error in ConnectionTest: %v", result.ErrorMessage)
	}
}
