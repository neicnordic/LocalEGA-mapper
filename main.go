// Package main contains the main logic of the "mapper" microservice.
package main

import (
	"crypto/tls"
	"database/sql"
	"encoding/json"
	_ "github.com/lib/pq"
	"github.com/streadway/amqp"
	"log"
	"os"
	"sync"
)

type mapping struct {
	StableId  string `json:"stableId"`
	DatasetId string `json:"datasetId"`
}

const defaultQueueName = "mappings"

var dbIn *sql.DB
var dbOut *sql.DB
var mappingMutex sync.Mutex

func main() {
	var err error

	dbIn, err = sql.Open("postgres", os.Getenv("DB_IN_CONNECTION"))
	failOnError(err, "Failed to connect to DB")

	dbOut, err = sql.Open("postgres", os.Getenv("DB_OUT_CONNECTION"))
	failOnError(err, "Failed to connect to DB")

	mq, err := amqp.DialTLS(os.Getenv("MQ_CONNECTION"), getTLSConfig())
	failOnError(err, "Failed to connect to RabbitMQ")

	channel, err := mq.Channel()
	failOnError(err, "Failed to create RabbitMQ channel")

	queueName := os.Getenv("QUEUE_NAME")
	if queueName == "" {
		queueName = defaultQueueName
	}

	deliveries, err := channel.Consume(
		queueName,
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
		processDelivery(delivery)
	}
}

func processDelivery(delivery amqp.Delivery) {
	mappingMutex.Lock()
	defer mappingMutex.Unlock()

	var mappings []mapping
	err := json.Unmarshal(delivery.Body, &mappings)
	if err != nil {
		log.Printf("%s: %s", "Failed to parse incoming message", err)
		return
	}

	transaction, err := dbOut.Begin()
	failOnError(err, "Failed to begin transaction")

	for _, mapping := range mappings {
		stableId := mapping.StableId
		fileId, err := selectFileIdByStableId(stableId)
		failOnError(err, "Failed to select fileId by stableId: "+stableId)

		_, err = transaction.Exec(
			"insert into local_ega_ebi.filedataset (file_id, dataset_stable_id) values ($1, $2)",
			fileId,
			mapping.DatasetId,
		)
		failOnError(err, "Failed to insert mapping")
	}

	err = transaction.Commit()
	failOnError(err, "Failed to commit transaction")

	log.Printf("Mappings stored: %v", mappings)
}

func selectFileIdByStableId(stableId string) (fileId int, err error) {
	err = dbIn.QueryRow("select id from local_ega.files where stable_id = $1", stableId).Scan(&fileId)
	return
}

func getTLSConfig() *tls.Config {
	tlsConfig := tls.Config{}
	if os.Getenv("VERIFY_CERT") == "true" {
		tlsConfig.InsecureSkipVerify = false
	} else {
		tlsConfig.InsecureSkipVerify = true
	}
	return &tlsConfig
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
