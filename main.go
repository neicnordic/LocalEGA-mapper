package main

import (
	"database/sql"
	"encoding/json"
	_ "github.com/lib/pq"
	"github.com/streadway/amqp"
	"log"
	"os"
	"sync"
)

type Mapping struct {
	FileId    string `json:"fileId"`
	DatasetId string `json:"datasetId"`
}

var mappingMutex sync.Mutex

func main() {
	db, err := sql.Open("postgres", os.Getenv("DB_CONNECTION"))
	failOnError(err, "Failed to connect to PostgreSQL")
	defer db.Close()

	mq, err := amqp.Dial(os.Getenv("MQ_CONNECTION"))
	failOnError(err, "Failed to connect to RabbitMQ")
	defer mq.Close()

	channel, err := mq.Channel()
	failOnError(err, "Failed to create RabbitMQ channel")

	deliveries, err := channel.Consume(
		os.Getenv("QUEUE_NAME"),
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to connect to queue")

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")

	for delivery := range deliveries {
		transaction, err := db.Begin()
		failOnError(err, "Failed to begin transaction")
		processDelivery(delivery, transaction)
	}
}

func processDelivery(delivery amqp.Delivery, transaction *sql.Tx) {
	mappingMutex.Lock()
	defer mappingMutex.Unlock()

	var mappings []Mapping
	err := json.Unmarshal(delivery.Body, &mappings)
	if err != nil {
		log.Printf("%s: %s", "Failed to parse incoming message", err)
		err := transaction.Rollback()
		failOnError(err, "Failed to rollback transaction")
		return
	}

	for _, mapping := range mappings {
		_, err := transaction.Exec(
			"insert into local_ega_ebi.filedataset (file_id, dataset_stable_id) values ($1, $2)",
			mapping.FileId,
			mapping.DatasetId,
		)
		failOnError(err, "Failed to insert mapping")
	}

	err = transaction.Commit()
	failOnError(err, "Failed to commit transaction")
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
