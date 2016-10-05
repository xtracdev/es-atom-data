package atom

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

type envConfig struct {
	DBUser     string
	DBPassword string
	DBHost     string
	DBPort     string
	DBSvc      string
}

func (ec *envConfig) MaskedConnectString() string {
	return fmt.Sprintf("%s/%s@//%s:%s/%s",
		ec.DBUser, "XXX", ec.DBHost, ec.DBPort, ec.DBSvc)
}

func (ec *envConfig) ConnectString() string {
	return fmt.Sprintf("%s/%s@//%s:%s/%s",
		ec.DBUser, ec.DBPassword, ec.DBHost, ec.DBPort, ec.DBSvc)
}

func NewEnvConfig() (*envConfig, error) {
	var configErrors []string

	user := os.Getenv("FEED_DB_USER")
	if user == "" {
		configErrors = append(configErrors, "Configuration missing FEED_DB_USER env variable")
	}

	password := os.Getenv("FEED_DB_PASSWORD")
	if password == "" {
		configErrors = append(configErrors, "Configuration missing FEED_DB_PASSWORD env variable")
	}

	dbhost := os.Getenv("FEED_DB_HOST")
	if dbhost == "" {
		configErrors = append(configErrors, "Configuration missing FEED_DB_HOST env variable")
	}

	dbPort := os.Getenv("FEED_DB_PORT")
	if dbPort == "" {
		configErrors = append(configErrors, "Configuration missing FEED_DB_PORT env variable")
	}

	dbSvc := os.Getenv("FEED_DB_SVC")
	if dbSvc == "" {
		configErrors = append(configErrors, "Configuration missing FEED_DB_SVC env variable")
	}

	if len(configErrors) != 0 {
		return nil, errors.New(strings.Join(configErrors, "\n"))
	}

	return &envConfig{
		DBUser:     user,
		DBPassword: password,
		DBHost:     dbhost,
		DBPort:     dbPort,
		DBSvc:      dbSvc,
	}, nil

}
