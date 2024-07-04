package main

import (
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"strings"

	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/viper"
)

type MariaDatabase struct {
	Config       *ConfigMysql
	Password     string
	DatabaseName string
	letterRunes  []rune
}

type ConfigMysql struct {
	Address         string `mapstructure:"address"`
	Username        string `mapstructure:"username"`
	DatabaseName    string `mapstructure:"databaseName"`
	EnvVariableName string `mapstructure:"envVariableName"`
	KeyName         string `mapstructure:"keyName"`
	ValueName       string `mapstructure:"valueName"`
}

func MariaDBGetDefaults(configReader *viper.Viper) {
	configReader.SetDefault("mysql.address", "localhost:3306")
	configReader.SetDefault("mysql.username", "kvdb")
	configReader.SetDefault("mysql.databaseName", "")
	configReader.SetDefault("mysql.envVariableName", BaseENVname+"_MYSQL_PASSWORD")
	configReader.SetDefault("mysql.keyName", "key")
	configReader.SetDefault("mysql.valueName", "value")
}

type DBHealth struct {
	Connection   bool   `json:"connection"`
	CreateTable  bool   `json:"createTable"`
	DeleteTable  bool   `json:"deleteTable"`
	CreateRow    bool   `json:"createRow"`
	SelectRow    bool   `json:"selectRow"`
	DeleteRow    bool   `json:"deleteRow"`
	ErrorMessage string `json:"errorMessage,omitempty"`
}

func (hc *DBHealth) ok() bool {
	return hc.Connection && hc.CreateTable && hc.DeleteTable && hc.CreateRow && hc.SelectRow && hc.DeleteRow
}

func (hc *DBHealth) log(logger *slog.Logger) {
	logger.Info("healthCheck",
		"connection", hc.Connection,
		"createTable", hc.CreateTable,
		"deleteTable", hc.DeleteTable,
		"createRow", hc.CreateRow,
		"selectRow", hc.SelectRow,
		"deleteRow", hc.DeleteRow)
}

type ErrNotFound struct {
	Value string
}

func (err *ErrNotFound) Error() string {
	return fmt.Sprintf("%v not found", err.Value)
}
func (MDB *MariaDatabase) Init() {

	Logger.Debug("Initializing MariaDB", "function", "Init", "struct", "MariaDatabase")
	if MDB.Config.DatabaseName == "" {
		MDB.DatabaseName = MDB.Config.Username
	} else {
		MDB.DatabaseName = MDB.Config.DatabaseName
	}
	MDB.Password = os.Getenv(MDB.Config.EnvVariableName)
	MDB.letterRunes = []rune("1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
}

func (MDB *MariaDatabase) RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = MDB.letterRunes[rand.Intn(len(MDB.letterRunes))]
	}
	return string(b)
}

func (MDB *MariaDatabase) ConnectionTest() DBHealth {
	var health DBHealth
	conn, err := MDB.CreateConnection()
	if err != nil {
		health.Connection = false
		health.ErrorMessage = err.Error()
		return health
	} else {
		health.Connection = true
	}

	defer conn.Close()
	tableName := MDB.RandStringRunes(10)
	keyName := MDB.RandStringRunes(10)
	value := MDB.RandStringRunes(30)

	err = MDB.CreateNamespace(conn, tableName)
	if err != nil {
		health.CreateTable = false
		health.ErrorMessage = err.Error()
		return health
	} else {
		health.CreateTable = true
	}

	err = MDB.Set(conn, tableName, keyName, value)
	if err != nil {
		health.CreateRow = false
		health.ErrorMessage = err.Error()
		return health
	} else {
		health.CreateRow = true
	}

	val, err := MDB.Get(conn, tableName, keyName)
	if err != nil {
		health.SelectRow = false
		health.ErrorMessage = err.Error()
		return health
	} else {
		if val != value {
			health.SelectRow = false
			health.ErrorMessage = fmt.Sprintf("Values do not match %v != %v", val, value)
			return health
		} else {
			health.SelectRow = true
		}
	}

	err = MDB.DeleteKey(conn, tableName, keyName)
	if err != nil {
		health.DeleteRow = false
		health.ErrorMessage = err.Error()
		return health
	} else {
		health.DeleteRow = true
	}

	err = MDB.DeleteNamespace(conn, tableName)
	if err != nil {
		health.DeleteTable = false
		health.ErrorMessage = err.Error()
		return health
	} else {
		health.DeleteTable = true
	}
	return health
}
func (MDB *MariaDatabase) CreateConnection() (*sql.DB, error) {
	connectionString := fmt.Sprintf("%v:%v@tcp(%v)/%v", MDB.Config.Username, MDB.Password, MDB.Config.Address, MDB.DatabaseName)
	return sql.Open("mysql", connectionString)
}

func (MDB *MariaDatabase) Set(conn *sql.DB, namespace string, key string, value interface{}) error {
	statement, err := conn.Prepare(fmt.Sprintf("INSERT INTO `%v` (`%v`, `%v`) VALUES (?, ?) ON DUPLICATE KEY UPDATE `%v`=?", namespace, MDB.Config.KeyName, MDB.Config.ValueName, MDB.Config.ValueName))
	if err != nil {
		return err
	}
	_, err = statement.Exec(key, value, value)
	if err != nil {
		return err
	}
	return nil
}

func (MDB *MariaDatabase) Get(conn *sql.DB, namespace string, key string) (string, error) {
	rows, err := conn.Query(fmt.Sprintf("select * from `%v` where `%v` = ? ", namespace, MDB.Config.KeyName), key)
	if err != nil {
		if strings.Contains(err.Error(), "Error 1146 (42S02)") {
			return "", &ErrNotFound{Value: namespace}
		}
		Logger.Error("Query failed with error", "function", "Get", "struct", "MariaDatabase", "namespace", namespace, "error", err)
		return "", err
	}
	defer rows.Close()
	var Key, Value string
	found := false
	for rows.Next() {
		err = rows.Scan(Key, Value)
		if err != nil {
			Logger.Error("Scan row failed with error", "function", "Get", "struct", "MariaDatabase", "namespace", namespace, "error", err)
			return "", err
		}
		found = true
	}
	if found {
		return Value, err
	} else {
		return "", &ErrNotFound{Value: key}
	}
}

func (MDB *MariaDatabase) DeleteKey(conn *sql.DB, namespace string, key string) error {
	stmt, err := conn.Prepare(fmt.Sprintf("delete from `%v` where `%v` = ?", namespace, MDB.Config.KeyName))
	if err != nil {
		Logger.Error("Prepare failed with error", "function", "DeleteKey", "struct", "MariaDatabase", "namespace", namespace, "key", key, "error", err)
		return err
	}
	_, err = stmt.Exec(key)
	if err != nil {
		Logger.Error("Exec failed with error", "function", "DeleteKey", "struct", "MariaDatabase", "namespace", namespace, "key", key, "error", err)
		return err
	}
	return nil
}

func (MDB *MariaDatabase) CreateNamespace(conn *sql.DB, namespace string) error {
	result, err := conn.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%v` ( `%v` CHAR(%v) PRIMARY KEY, `%v` VARCHAR(%v) NOT NULL) ENGINE = InnoDB; ", namespace, MDB.Config.KeyName, KeyMaxLength, MDB.Config.ValueName, ValueMaxLength))
	Logger.Debug("Create table if not exists", "function", "createTable", "struct", "MariaDatabase", "namespace", namespace, "result", result)
	if err != nil {
		Logger.Error("Error creating table", "function", "createTable", "struct", "MariaDatabase", "namespace", namespace, "error", err)
		return err
	}
	return nil
}

func (MDB *MariaDatabase) DeleteNamespace(conn *sql.DB, namespace string) error {
	_, err := conn.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%v`", namespace))
	if err != nil {
		Logger.Error("Exec failed with error", "function", "Delete", "struct", "MariaDatabase", "namespace", namespace, "error", err)
		return err
	}
	return nil
}
