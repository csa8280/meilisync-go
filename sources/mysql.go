package sources

import (
	"errors"
	"fmt"
	"log"
	config2 "meilisync-go/config"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/meilisearch/meilisearch-go"
)

type MyEventHandler struct {
	canal.DummyEventHandler
	client      *meilisearch.Client
	config      config2.Config
	batchMap    map[string][]map[string]interface{} // Map of index name to batch
	batchDelete map[string][]string                 // Map of index name to batchDelete
	batchSize   int
	lastSend    time.Time
}

// ParseToJson converts a RowsEvent and row data into a JSON map.
func ParseToJson(e *canal.RowsEvent, row []interface{}) map[string]interface{} {
	jsonData := make(map[string]interface{})
	columnNames := e.Table.Columns
	for i, columnValue := range row {
		switch value := columnValue.(type) {
		case []byte:
			jsonData[columnNames[i].Name] = string(value)
		default:
			jsonData[columnNames[i].Name] = value
		}
	}
	return jsonData
}

func (h *MyEventHandler) OnRow(e *canal.RowsEvent) error {
	n, k := 0, 1

	if e.Action == canal.UpdateAction {
		n, k = 1, 2
	}

	// Check if the table is configured for synchronization
	if syncConfig, ok := h.config.Tables[e.Table.Name]; ok {
		for i := n; i < len(e.Rows); i += k {
			tableName := e.Table.Name
			row := e.Rows[i]

			rowData := ParseToJson(e, row)
			switch e.Action {
			case canal.DeleteAction:
				if h.batchDelete[tableName] == nil {
					h.batchDelete[tableName] = []string{}
				}
				h.batchDelete[tableName] = append(h.batchDelete[tableName], rowData[syncConfig.PrimaryKey].(string))
				h.batchSize += 1
			case canal.UpdateAction, canal.InsertAction:
				if h.batchMap[tableName] == nil {
					h.batchMap[tableName] = []map[string]interface{}{}
				}
				h.batchMap[tableName] = append(h.batchMap[tableName], rowData)
				h.batchSize += 1
			}
		}

		if h.batchSize >= h.config.MeiliSearch.InsertSize {
			err := SendBatches(h)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func SendBatches(h *MyEventHandler) error {
	fmt.Printf("sent at %v batch size - %v \n", h.batchSize, time.Now().Format(time.RFC822))
	for k, v := range h.batchMap {
		indexName := h.config.Tables[k].Index
		index := h.client.Index(indexName)
		_, err := index.AddDocuments(v, h.config.Tables[k].PrimaryKey)
		if err != nil {
			return err
		}

		if toDelete := h.batchDelete[k]; toDelete != nil {
			_, err = index.DeleteDocuments(toDelete)
			if err != nil {
				return err
			}
			h.batchDelete = nil
		}
	}
	h.batchMap = nil
	h.lastSend = time.Now()

	return nil
}

func checkAndSendBatchesRegularly(h *MyEventHandler, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Check if it's time to send batches
			if time.Since(h.lastSend) >= interval {
				if err := SendBatches(h); err != nil {
					log.Println("Error sending batches:", err)
				}
			}
		}
	}
}

func (h *MyEventHandler) String() string {
	return "MyEventHandler"
}

func InitSource(msClient *meilisearch.Client, conf config2.Config) {
	cfg := configureCanal(conf)
	c, parsedPos := initializeCanal(cfg, conf)
	defer SaveProgress(c, conf)

	h := &MyEventHandler{
		client:   msClient,
		config:   conf,
		lastSend: time.Now(), // Initialize lastSend to the current time
	}

	c.SetEventHandler(h)

	go startSaver(conf, c)
	go checkAndSendBatchesRegularly(h, time.Duration(conf.MeiliSearch.InsertInterval))

	err := c.RunFrom(parsedPos)
	if err != nil {
		log.Fatal(err)
	}
}

func startSaver(conf config2.Config, c *canal.Canal) {
	saveProgressTimer := time.NewTicker(time.Duration(conf.ProgressConfig.SaveInterval))
	defer saveProgressTimer.Stop()

	for {
		select {
		case <-saveProgressTimer.C:
			SaveProgress(c, conf)
		}
	}
}

func configureCanal(conf config2.Config) *canal.Config {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%v:%v", conf.Source.Host, conf.Source.Port)
	cfg.User = conf.Source.User
	cfg.Password = conf.Source.Password
	cfg.Flavor = conf.Source.Type

	cfg.ExcludeTableRegex = []string{"mysql\\..*"}

	// Include tables based on configuration
	tablesRegex := make([]string, 0)
	for k := range conf.Tables {
		tablesRegex = append(tablesRegex, fmt.Sprintf(".*\\.%v", k))
	}

	cfg.IncludeTableRegex = []string{
		strings.Join(tablesRegex, "|"),
	}
	return cfg
}

func initializeCanal(cfg *canal.Config, conf config2.Config) (*canal.Canal, mysql.Position) {
	c, err := canal.NewCanal(cfg)
	if err != nil {
		log.Fatal(err)
	}

	parsedPos, err := c.GetMasterPos()
	if err != nil {
		log.Fatal("Couldn't initialize parsedPos.")
	}

	fileData, err := os.ReadFile(conf.ProgressConfig.Location)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Fatalf("Couldn't continue from file. %v", err)
		} else {
			log.Println("No progress file found - starting from the beginning.")
		}
	} else {
		regex := regexp.MustCompile(`\(([^,]+),\s*(\d+)\)`)
		match := regex.FindStringSubmatch(string(fileData))
		if len(match) == 3 {
			name := match[1]
			pos := match[2]
			log.Printf("Binlog name: %s. Binlog pos: %s\n", name, pos)
			parsedPos.Name = name
			posInt, _ := strconv.Atoi(pos)
			parsedPos.Pos = uint32(posInt)
		} else {
			log.Printf("No match found in the input %v. \n", string(fileData))
		}
	}

	if err != nil {
		log.Print("Couldn't continue from file.", err)
	}

	return c, parsedPos
}

func SaveProgress(c *canal.Canal, conf config2.Config) {
	pos, err := c.GetMasterPos()
	if err != nil {
		log.Print("Error while saving progress", err)
	}
	sPos := pos.String()
	err = os.WriteFile(conf.ProgressConfig.Location, []byte(sPos), 0644)
	if err != nil {
		log.Printf("Error while saving progress. Current - %v. %v", sPos, err)
	}
}
