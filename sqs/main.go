package main

import (
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func main() {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	client := sqs.NewFromConfig(cfg)

	input := &sqs.ListQueuesInput{}
	queues, err := client.ListQueues(context.TODO(), input)
	if err != nil {
		log.Fatal(err)
		return
	}
	for i, url := range queues.QueueUrls {
		fmt.Printf("%d: %s\n", i+1, url)
	}

	messageBody := "this is test message into queue"
	groupID := "test"
	deduplicationID := "cb4a9e53-cb97-4591-9683-96246ca5574e"
	sendMessageInput := &sqs.SendMessageInput{
		MessageBody:            &messageBody,
		QueueUrl:               &queues.QueueUrls[0],
		MessageDeduplicationId: &deduplicationID,
		MessageGroupId:         &groupID,
		MessageAttributes: map[string]types.MessageAttributeValue{
			"Title": {
				DataType:    aws.String("String"),
				StringValue: aws.String("The Whistler"),
			},
			"Author": {
				DataType:    aws.String("String"),
				StringValue: aws.String("John Grisham"),
			},
			"WeeksOn": {
				DataType:    aws.String("Number"),
				StringValue: aws.String("6"),
			},
		},
	}
	output, err := client.SendMessage(context.TODO(), sendMessageInput)
	if err != nil {
		log.Fatal(err)
		return
	}
	fmt.Printf("%+v\n", output)
}
