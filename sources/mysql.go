package sources

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/schema"
	config2 "meilisync-go/config"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/meilisearch/meilisearch-go"
)

const mysqlDateFormat = "2006-01-02"

type MyEventHandler struct {
	canal.DummyEventHandler
	client      *meilisearch.Client
	config      config2.Config
	batchMap    map[string][]map[string]interface{} // Map of index name to batch
	batchDelete map[string][]string                 // Map of index name to batchDelete
	batchSize   int
	lastSend    time.Time
}

func makeReqColumnData(col *schema.TableColumn, value interface{}) interface{} {
	switch col.Type {
	case schema.TYPE_ENUM:
		switch value := value.(type) {
		case int64:
			// for binlog, ENUM may be int64, but for dump, enum is string
			eNum := value - 1
			if eNum < 0 || eNum >= int64(len(col.EnumValues)) {
				// we insert invalid enum value before, so return empty
				log.Println("invalid binlog enum index %d, for enum %v", eNum, col.EnumValues)
				return ""
			}

			return col.EnumValues[eNum]
		}
	case schema.TYPE_SET:
		switch value := value.(type) {
		case int64:
			// for binlog, SET may be int64, but for dump, SET is string
			bitmask := value
			sets := make([]string, 0, len(col.SetValues))
			for i, s := range col.SetValues {
				if bitmask&int64(1<<uint(i)) > 0 {
					sets = append(sets, s)
				}
			}
			return strings.Join(sets, ",")
		}
	case schema.TYPE_BIT:
		switch value := value.(type) {
		case string:
			// for binlog, BIT is int64, but for dump, BIT is string
			// for dump 0x01 is for 1, \0 is for 0
			if value == "\x01" {
				return int64(1)
			}

			return int64(0)
		}
	case schema.TYPE_STRING:
		switch value := value.(type) {
		case []byte:
			return string(value[:])
		}
	case schema.TYPE_JSON:
		var f interface{}
		var err error
		switch v := value.(type) {
		case string:
			err = json.Unmarshal([]byte(v), &f)
		case []byte:
			err = json.Unmarshal(v, &f)
		}
		if err == nil && f != nil {
			return f
		}
	case schema.TYPE_DATETIME, schema.TYPE_TIMESTAMP:
		switch v := value.(type) {
		case string:
			vt, err := time.ParseInLocation(mysql.TimeFormat, string(v), time.Local)
			if err != nil || vt.IsZero() { // failed to parse date or zero date
				return nil
			}
			return vt.Format(time.RFC3339)
		}
	case schema.TYPE_DATE:
		switch v := value.(type) {
		case string:
			vt, err := time.Parse(mysqlDateFormat, string(v))
			if err != nil || vt.IsZero() { // failed to parse date or zero date
				return nil
			}
			return vt.Format(mysqlDateFormat)
		}
	}

	return value
}

// ParseToJson converts a RowsEvent and row data into a JSON map.
func ParseToJson(e *canal.RowsEvent, row []interface{}) map[string]interface{} {
	jsonData := make(map[string]interface{})
	columnNames := e.Table.Columns
	for i, columnValue := range row {
		col := &columnNames[i]
		value := makeReqColumnData(col, columnValue)
		jsonData[columnNames[i].Name] = value
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
				h.batchDelete[tableName] = append(h.batchDelete[tableName], fmt.Sprintf("%v", rowData[syncConfig.PrimaryKey]))
				h.batchSize += 1
			case canal.UpdateAction, canal.InsertAction:
				if v := rowData["deleted_at"]; v != nil {
					if h.batchDelete[tableName] == nil {
						h.batchDelete[tableName] = []string{}
					}
					h.batchDelete[tableName] = append(h.batchDelete[tableName], fmt.Sprintf("%v", rowData[syncConfig.PrimaryKey]))
					h.batchSize += 1
				} else {
					if h.batchMap[tableName] == nil {
						h.batchMap[tableName] = []map[string]interface{}{}
					}
					h.batchMap[tableName] = append(h.batchMap[tableName], rowData)
					h.batchSize += 1
				}
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
	if h.batchSize == 0 {
		return nil
	}
	for k, v := range h.batchMap {
		indexName := h.config.Tables[k].Index
		index := h.client.Index(indexName)
		_, err := index.AddDocuments(v, h.config.Tables[k].PrimaryKey)
		if err != nil {
			return err
		}

		if toDelete := h.batchDelete[k]; toDelete != nil {
			fmt.Printf("Tried to delete %v", toDelete)
			_, err = index.DeleteDocumentsByFilter("deleted_at IS NOT NULL")
			if err != nil {
				return err
			}
		}
	}

	for k, v := range h.batchDelete {
		indexName := h.config.Tables[k].Index
		index := h.client.Index(indexName)
		fmt.Printf("Tried to delete %v", v)
		_, err := index.DeleteDocuments(v)
		if err != nil {
			return err
		}
	}

	h.batchDelete = make(map[string][]string)
	h.batchMap = make(map[string][]map[string]interface{})
	h.lastSend = time.Now()
	h.batchSize = 0
	return nil
}

func checkAndSendBatchesRegularly(h *MyEventHandler, interval time.Duration) {
	ticker := time.NewTicker(interval * time.Second)
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
		client:      msClient,
		config:      conf,
		lastSend:    time.Now(), // Initialize lastSend to the current time
		batchMap:    make(map[string][]map[string]interface{}),
		batchDelete: make(map[string][]string),
	}

	c.SetEventHandler(h)

	go startSaver(conf, c)
	go checkAndSendBatchesRegularly(h, time.Duration(conf.MeiliSearch.InsertInterval))
	if conf.ProgressConfig.SkipDump {
		parsedPos, err := c.GetMasterPos()
		if err != nil {
			log.Fatal(err)
		}
		err = c.RunFrom(parsedPos)
	} else {

		if (parsedPos == mysql.Position{}) {
			err := c.Run()
			if err != nil {
				log.Fatal(err)
			}
		} else {
			err := c.RunFrom(parsedPos)
			if err != nil {
				log.Fatal(err)
			}
		}

	}

}

func startSaver(conf config2.Config, c *canal.Canal) {
	saveProgressTimer := time.NewTicker(time.Duration(conf.ProgressConfig.SaveInterval) * time.Second)
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
	cfg.Dump.DiscardErr = true
	cfg.ExcludeTableRegex = []string{"mysql\\..*"}

	// Include tables based on configuration
	tablesRegex := make([]string, 0)
	for k := range conf.Tables {
		tablesRegex = append(tablesRegex, fmt.Sprintf("%v\\.%v", conf.Source.Database, k))
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

	var parsedPos mysql.Position

	fileData, err := os.ReadFile(conf.ProgressConfig.Location)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Fatalf("Couldn't continue from file. %v", err)
		} else {
			log.Println("No progress file found - starting from the beginning.")
			return c, mysql.Position{}
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
