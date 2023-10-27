# Meilisync-Go

Meilisync-Go is a Go program designed to synchronize data from a MySQL database to a Meilisearch instance. It provides a simple and efficient way to keep your Meilisearch index up-to-date with the data stored in your MySQL database.

## Features

- Real-time synchronization: Continuously updates Meilisearch with changes from MySQL.
- Selective indexing: Choose specific tables or columns to sync.
- Configurable: Easily configure your MySQL and Meilisearch connection settings.

## Getting Started

### Prerequisites

Before you begin, ensure you have the following requirements:

- Access to a running MySQL database.
- Access to a Meilisearch instance.
- A working docker installation
### Installation

Meilisync-Go can be run using Docker. A Docker image is available on Docker Hub at [patsore/meilisync-go](https://hub.docker.com/r/patsore/meilisync-go). To get started, follow these steps:

1. Pull the Docker image from Docker Hub:

```shell
docker pull patsore/meilisync-go:main
```
Set up your configuration file on your local machine. Create a config.toml file with your MySQL and Meilisearch connection settings. (see example [here](config.toml))

Run Meilisync-Go using Docker, specifying the location of your configuration file and the volume where progress should be saved. Replace /my/meilisync/location/ with the actual path to your configuration file on your local machine:

```shell
docker run -d -e MEILISYNC_CONFIG_LOCATION=/app/config.toml -v /my/meilisync/location/:/app patsore/meilisync-go:main
```

Meilisync-Go will run inside the Docker container, using the configuration file you provided. It will continuously synchronize your MySQL data to the Meilisearch index according to the configuration, and save the progress to the folder you mounted.

Monitor the synchronization process and ensure that your data is being indexed correctly.

# License
See [LICENSE](LICENSE) for this project's license.