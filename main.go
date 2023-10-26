package main

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/meilisearch/meilisearch-go"
	"log"
	conf "meilisync-go/config"
	"meilisync-go/sources"
	"os"
)

var config conf.Config
var client *meilisearch.Client

func init() {

	err := conf.ReadConfig(os.Getenv("MEILISYNC_CONFIG_LOCATION"), &config)
	if err != nil {
		log.Fatalf("Error reading configuration: %s", err)
	}

	// Create a Meilisearch client
	client = meilisearch.NewClient(meilisearch.ClientConfig{
		Host:   config.MeiliSearch.APIURL,
		APIKey: config.MeiliSearch.APIKey,
	})

	if !client.IsHealthy() {
		log.Fatal("Couldn't connect to meilisearch client!")
	}

	if err != nil {
		log.Fatal(err)
	}

}

func main() {

	sources.InitSource(client, config)

}
