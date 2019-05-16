package main

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"os"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

func getMongoCollection(mongoURL, dbName, collectionName string) *mongo.Collection {

	//认证参数设置，否则连不上
	opts := &options.ClientOptions{}
	opts.SetAuth(options.Credential{
		AuthMechanism:"SCRAM-SHA-1",
		AuthSource:"test",
		Username:"test",
		Password:"123456"})

	//链接mongo服务
	client, err := mongo.Connect(getContext(), options.Client().ApplyURI(mongoURL),opts)
	if err != nil {
		log.Fatal(err)
	}

	// Check the connection
	err = client.Ping(context.Background(), nil)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Connected to MongoDB ... !!")

	db := client.Database(dbName)
	collection := db.Collection(collectionName)
	return collection
}

func checkErr(err error) {
	if err != nil {
		if err == mongo.ErrNoDocuments {
			fmt.Println("没有查到数据")
			os.Exit(0)
		} else {
			fmt.Println(err)
			os.Exit(0)
		}

	}
}

func getContext() (ctx context.Context) {
	ctx, _ = context.WithTimeout(context.Background(), 10*time.Second)
	return
}

func getKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaURL},
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
}

func main() {

	// get Mongo db Collection using environment variables.
	mongoURL := os.Getenv("mongoURL")
	dbName := os.Getenv("dbName")
	collectionName := os.Getenv("collectionName")

	mongoURL="mongodb://远程服务器ip:27017"
	dbName = "test"
	collectionName = "test"
	collection := getMongoCollection(mongoURL, dbName, collectionName)

	// get kafka reader using environment variables.
	kafkaURL := os.Getenv("kafkaURL")
	topic := os.Getenv("topic")
	kafkaURL = "localhost:9092"
	topic = "my-test"

	groupID := os.Getenv("groupID")
	reader := getKafkaReader(kafkaURL, topic, groupID)

	defer reader.Close()

	fmt.Println("start consuming ... !!")

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		insertResult, err := collection.InsertOne(context.Background(), msg)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("Inserted a single document: ", insertResult.InsertedID)
	}
}
