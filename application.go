package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/spf13/viper"
)

const (
	healthPath            = "/health"
	BaseENVname           = "DBTEST"
	KeyMaxLength   uint16 = 64
	ValueMaxLength uint16 = 16000
)

var (
	configFileName string
	Logger         *slog.Logger
)

type Application struct {
	Config ConfigType
	DB     *MariaDatabase
}

type ConfigType struct {
	Logging ConfigLogging `mapstructure:"logging"`
	Port    string        `mapstructure:"port"`
	Mysql   ConfigMysql   `mapstructure:"mysql"`
}

type ConfigLogging struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

func ConfigRead(configFileName string, configOutput *ConfigType) *viper.Viper {
	configReader := viper.New()
	configReader.SetConfigName(configFileName)
	configReader.SetConfigType("yaml")
	configReader.AddConfigPath("/app/")
	configReader.AddConfigPath(".")
	configReader.SetEnvPrefix(BaseENVname)
	MariaDBGetDefaults(configReader)
	configReader.SetDefault("logging.level", "Debug")
	configReader.SetDefault("logging.format", "text")
	configReader.SetDefault("port", 8080)

	err := configReader.ReadInConfig() // Find and read the config file
	if err != nil {                    // Handle errors reading the config file
		panic(fmt.Errorf("fatal error config file: %w", err))
	}
	configReader.AutomaticEnv()
	configReader.Unmarshal(configOutput)
	return configReader
}

func (App *Application) HealthActuator(w http.ResponseWriter, r *http.Request) {
	logger := Logger.With(slog.Any("remoteAddr", r.RemoteAddr)).With(slog.Any("method", r.Method))

	reply := App.DB.ConnectionTest()
	reply.log(logger)
	logger.Info("health", "status", http.StatusOK)
	if !reply.ok() {
		w.WriteHeader(http.StatusInternalServerError)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(reply)
}

func (App *Application) setupLogging() {
	logLevel := strings.ToLower(App.Config.Logging.Level)
	logFormat := strings.ToLower(App.Config.Logging.Format)
	loggingLevel := new(slog.LevelVar)
	switch logLevel {
	case "debug":
		loggingLevel.Set(slog.LevelDebug)
	case "warn":
		loggingLevel.Set(slog.LevelWarn)
	case "error":
		loggingLevel.Set(slog.LevelError)
	default:
		loggingLevel.Set(slog.LevelInfo)
	}
	switch logFormat {
	case "json":
		Logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: loggingLevel}))
	default:
		Logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: loggingLevel}))
	}
	Logger.Info("Logging started with options", "format", App.Config.Logging.Format, "level", App.Config.Logging.Level, "function", "setupLogging")
	slog.SetDefault(Logger)
}

func setupTestlogging() {
	loggingLevel := new(slog.LevelVar)
	loggingLevel.Set(slog.LevelDebug)
	Logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: loggingLevel, AddSource: true}))
}

func (App *Application) RootController(w http.ResponseWriter, r *http.Request) {
	logger := Logger.With(slog.Any("remoteAddr", r.RemoteAddr)).With(slog.Any("method", r.Method), "path", r.URL.EscapedPath())
	logger.Info("request to root")
	http.Redirect(w, r, healthPath, http.StatusSeeOther)
}

func (App *Application) BadRequestHandler(logger *slog.Logger, w http.ResponseWriter) {
	logger.Info("Bad Request", "status", 400)
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusBadRequest)
	w.Write([]byte("400 Bad Request"))
}

func main() {
	flag.StringVar(&configFileName, "config", "config", "Use a different config file name")
	flag.Parse()
	App := new(Application)
	ConfigRead(configFileName, &App.Config)
	// Logging setup
	App.setupLogging()
	App.DB = &MariaDatabase{Config: &App.Config.Mysql}
	App.DB.Init()
	//App.Auth.Init(App.Config)

	http.HandleFunc(healthPath, http.HandlerFunc(App.HealthActuator))

	Logger.Info(fmt.Sprintf("Serving on port %v", App.Config.Port))
	err := http.ListenAndServe(":"+App.Config.Port, nil)
	if err != nil {
		Logger.Error("Fatal", "Error", err)
	}

	Logger.Info("Server Stopped")
}
