package mssqldatalayer

import (
	"fmt"
	"os"
	"strings"

	"github.com/mimiro-io/mssqldatalayer/internal/conf"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func NewEnv() *conf.Env {
	profile, found := os.LookupEnv("PROFILE")
	if !found {
		profile = "local"
	}

	service, _ := os.LookupEnv("SERVICE_NAME")
	logger := getLogger(profile, zapcore.InfoLevel, service) // add a default logger while loading the env
	logger.Infof("Loading env: %s", profile)

	parseEnv(profile, logger)

	logger.Infof("Config location: %s", viper.GetString("CONFIG_LOCATION"))

	return &conf.Env{
		Logger:          logger,
		Env:             profile,
		Port:            viper.GetString("SERVER_PORT"),
		ConfigLocation:  viper.GetString("CONFIG_LOCATION"),
		RefreshInterval: viper.GetString("CONFIG_REFRESH_INTERVAL"),
		ServiceName:     viper.GetString("SERVICE_NAME"),
		User: conf.User{
			UserName: viper.GetString("MSSQL_DB_USER"),
			Password: viper.GetString("MSSQL_DB_PASSWORD"),
		},
		Auth: &conf.AuthConfig{
			WellKnown:     viper.GetString("TOKEN_WELL_KNOWN"),
			Audience:      viper.GetString("TOKEN_AUDIENCE"),
			AudienceAuth0: viper.GetString("TOKEN_AUDIENCE_AUTH0"),
			Issuer:        viper.GetString("TOKEN_ISSUER"),
			IssuerAuth0:   viper.GetString("TOKEN_ISSUER_AUTH0"),
			Middleware:    viper.GetString("AUTHORIZATION_MIDDLEWARE"),
		},
	}
}

func parseEnv(env string, logger *zap.SugaredLogger) {
	viper.SetConfigType("env")
	viper.AddConfigPath(".")
	viper.SetDefault("SERVER_PORT", "8080")
	viper.SetDefault("LOG_LEVEL", "INFO")
	viper.SetDefault("CONFIG_REFRESH_INTERVAL", "@every 60s")
	viper.SetDefault("SERVICE_NAME", "datahub-mssql-datalayer")
	viper.AutomaticEnv()

	viper.SetDefault("CONFIG_LOCATION", fmt.Sprintf("file://%s", ".config.json"))

	// read the .env file first
	viper.SetConfigName(".env")
	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		logger.DPanicf("Fatal error config file: %s", err)
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}

	logger.Infof("Reading config file %s", viper.GetViper().ConfigFileUsed())

	viper.SetConfigName(fmt.Sprintf(".env-%s", env))
	err = viper.MergeInConfig()
	if err != nil {
		logger.Warnf("Could not find .env-%s", env)
	} else {
		logger.Infof("Reading config file %s", viper.GetViper().ConfigFileUsed())
	}
}

func getLogger(env string, level zapcore.Level, serviceName string) *zap.SugaredLogger {
	var slogger *zap.SugaredLogger
	switch env {
	case "test":
		slogger = zap.NewNop().Sugar()
	case "local":
		cfg := zap.Config{
			Level:            zap.NewAtomicLevelAt(level),
			Development:      true,
			Encoding:         "console",
			EncoderConfig:    zap.NewDevelopmentEncoderConfig(),
			OutputPaths:      []string{"stderr"},
			ErrorOutputPaths: []string{"stderr"},
		}
		logger, _ := cfg.Build()
		slogger = logger.Sugar()
	default:
		cfg := zap.Config{
			Level:       zap.NewAtomicLevelAt(level),
			Development: false,
			Sampling: &zap.SamplingConfig{
				Initial:    100,
				Thereafter: 100,
			},
			Encoding:         "json",
			EncoderConfig:    zap.NewProductionEncoderConfig(),
			OutputPaths:      []string{"stderr"},
			ErrorOutputPaths: []string{"stderr"},
		}

		logger, _ := cfg.Build()
		slogger = logger.With(zap.String("service", serviceName), zap.String("source", "go")).Sugar() // reconfigure with default field
	}

	return slogger
}

func getLogLevel(level string) zapcore.Level {
	switch strings.ToUpper(level) {
	case "DEBUG":
		return zapcore.DebugLevel
	case "INFO":
		return zapcore.InfoLevel
	case "WARN":
		return zapcore.WarnLevel
	case "ERROR":
		return zapcore.ErrorLevel
	default:
		return zapcore.InfoLevel
	}
}
