package config

import (
	"github.com/spf13/viper"
)

type MeiliSearchConfig struct {
	APIURL         string `mapstructure:"api_url"`
	APIKey         string `mapstructure:"api_key"`
	InsertSize     int    `mapstructure:"insert_size"`
	InsertInterval int    `mapstructure:"insert_interval"`
}

type SourceConfig struct {
	Type     string `mapstructure:"type"`
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	Database string `mapstructure:"database"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
}

type ProgressConfig struct {
	Location string `mapstructure:"location"`
}

type SyncConfig struct {
	PrimaryKey string   `mapstructure:"primary_key"`
	Index      string   `mapstructure:"index"`
	Table      string   `mapstructure:"source"`
	Fields     []string `mapstructure:"fields"`
}

type Config struct {
	MeiliSearch    MeiliSearchConfig `mapstructure:"meilisearch"`
	Source         SourceConfig      `mapstructure:"source"`
	Sync           []SyncConfig      `mapstructure:"sync"`
	ProgressConfig ProgressConfig    `mapstructure:"progress"`
	Tables         map[string]SyncConfig
}

func ReadConfig(from string, into *Config) error {
	viper.SetConfigFile(from)
	if err := viper.ReadInConfig(); err != nil {
		return err
	}

	if err := viper.Unmarshal(into); err != nil {
		return err
	}
	into.Tables = make(map[string]SyncConfig)
	for _, syncConfig := range into.Sync {
		into.Tables[syncConfig.Table] = syncConfig
	}

	return nil
}
