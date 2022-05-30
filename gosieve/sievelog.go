package main
import (
	"encoding/csv"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"encoding/json"
	"time"
	"strconv"
	"os"
	"github.com/gocarina/gocsv"
)

type slog struct {
	Querier string `json:"querier"`
	Operation string `json:"operation"`
	Result string `json:"result"`
	Delete string `json:"delete"`
}
var fname string = "log" + strconv.FormatInt(time.Now().Unix(), 10) + ".csv"

func loopKeep(id string) []*slog{
	var keeps []*slog
	in, err := os.Open(fname)
    if err != nil {
        panic(err)
    }

    logs := []*slog{}

    if err := gocsv.UnmarshalFile(in, &logs); err != nil {
        panic(err)
    }
    for _, log := range logs {
        if log.Querier != id {
			keeps = append(keeps, log)
		}
    }
	return keeps
}

func logDel(id string) {
	f, err := os.OpenFile(fname, os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	listKeep := loopKeep(id)
	writer := csv.NewWriter(f)
	var data[][] string
	for _, log := range listKeep {
		column := []string{log.Querier, log.Operation, log.Result}
		data = append(data, column)
	}
	writer.WriteAll(data)
	
}

func logOp(log slog) {
	f, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}
	column := []string{log.Querier, log.Operation, log.Result}
	writer := csv.NewWriter(f)
	writer.Write(column)
	writer.Flush()
	

}
func main() {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	c.Subscribe("logResults", nil)

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			// fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
			var log slog
			json.Unmarshal([]byte(msg.Value), &log)
			if log.Delete == "false" { 
				// log
				logOp(log)
			} else {
				// delete log
				fmt.Println("DELETE")
				logDel(log.Querier)
			}
			// fmt.Println(log.Querier, log.Operation, log.Result, log.Delete)
		} else {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

	c.Close()
}