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
	"github.com/meilisearch/meilisearch-go"
)

type MyEventHandler struct {
	canal.DummyEventHandler
	client      *meilisearch.Client
	config      config2.Config
	batch       []map[string]interface{}
	batchDelete []string
	lastSend    time.Time
}

func ParseToJson(e *canal.RowsEvent, row []interface{}) (jsonData map[string]interface{}) {
	jsonData = make(map[string]interface{})
	columnNames := e.Table.Columns
	for i, columnValue := range row {
		switch value := columnValue.(type) {
		case []byte:
			{
				jsonData[columnNames[i].Name] = string(value)
			}
		case interface{}:
			{
				jsonData[columnNames[i].Name] = value
			}

		}
	}
	return jsonData
}

func (h *MyEventHandler) OnRow(e *canal.RowsEvent) error {
	var n = 0
	var k = 1

	if e.Action == canal.UpdateAction {
		n = 1
		k = 2
	}

	//not forget to add skipped lines to progress cursor
	if syncConfig, ok := h.config.Tables[e.Table.Name]; ok {

		index := h.client.Index(syncConfig.Index)

		for i := n; i < len(e.Rows); i += k {

			row := ParseToJson(e, e.Rows[i])
			switch e.Action {
			case canal.DeleteAction:
				{
					h.batchDelete = append(h.batchDelete, row[syncConfig.PrimaryKey].(string))
				}
			case canal.UpdateAction:
				{
					h.batch = append(h.batch, row)
				}
			case canal.InsertAction:
				{
					h.batch = append(h.batch, row)
				}
			}
		}
		//fmt.Println(rows[0])
		//fmt.Println(syncConfig.PrimaryKey)
		if len(h.batch) >= h.config.MeiliSearch.InsertSize || int(time.Since(h.lastSend).Seconds()) > h.config.MeiliSearch.InsertInterval {
			h.lastSend = time.Now()
			fmt.Printf("sent at %v batch size and %v delete size - %v \n", len(h.batch), len(h.batchDelete), h.lastSend)
			// Send the batch to MeiliSearch
			_, err := index.AddDocuments(h.batch, syncConfig.PrimaryKey)
			if err != nil {
				log.Fatal(err)
			}
			if h.batchDelete != nil {
				_, err = index.DeleteDocuments(h.batchDelete)
				if err != nil {
					log.Fatal(err)
				}
				h.batchDelete = nil
			}
			// Clear the batch
			h.batch = nil
		}
	}
	return nil
}

func (h *MyEventHandler) String() string {
	return "MyEventHandler"
}

func InitSource(msClient *meilisearch.Client, conf config2.Config) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%v:%v", conf.Source.Host, conf.Source.Port)
	cfg.User = conf.Source.User
	cfg.Password = conf.Source.Password
	cfg.Flavor = conf.Source.Type

	cfg.ExcludeTableRegex = []string{"mysql\\..*"}
	cfg.IncludeTableRegex = func() []string {
		tables := conf.Tables
		tablesRegex := make([]string, 0)

		for k, _ := range tables {
			tablesRegex = append(tablesRegex, fmt.Sprintf(".*\\.%v", k))
		}
		singleRegex := []string{
			strings.Join(tablesRegex, "|"),
		}
		log.Print(singleRegex)
		return singleRegex
	}()

	c, err := canal.NewCanal(cfg)
	if err != nil {
		log.Fatal(err)
	}

	parsedPos, err := c.GetMasterPos()
	if err != nil {
		log.Fatal("Couldn't init parsedPos.")
	}

	fileData, err := os.ReadFile(conf.ProgressConfig.Location)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Fatalf("Couldn't continue from file. %v", err)
		} else {
			log.Println("No progress file found - starting from beginning.")
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

	c.SetEventHandler(&MyEventHandler{
		client: msClient,
		config: conf,
	})
	go func() {
		for {
			SaveProgress(c, conf)
		}
	}()
	defer SaveProgress(c, conf)
	err = c.RunFrom(parsedPos)
	if err != nil {
		log.Fatal(err)
	}
}

func SaveProgress(c *canal.Canal, conf config2.Config) {
	pos, err := c.GetMasterPos()
	if err != nil {
		log.Print("error while saving progress", err)
	}
	sPos := pos.String()
	err = os.WriteFile(conf.ProgressConfig.Location, []byte(sPos), 0644)
	if err != nil {
		log.Printf("error while saving progress. current - %v. %v", sPos, err)
	}
}
