package orders


// Buna bakarak worker pool implemente et!
// https://github.com/gammazero/workerpool/blob/master/workerpool.go

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/google/uuid"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4/pgxpool"
	"strconv"
)

const (
	queueUrl = "https://sqs.eu-west-1.amazonaws.com/236584826472/StandardQueue"
	linger int64 = 2
)

type Order struct {
	Id string  `json:",omitempty"`
	CustomerId int `json:"customerId" validate:"required"`
	ProductId int `json:"productId" validate:"required"`
	OrderDesc string `json:"orderDesc" validate:"required"`
	ReceiptHandle *string `json:"-"`
	NumberOfOrders int
}

type OrderService struct {
	Dbpool *pgxpool.Pool
	Sqs sqsiface.SQSAPI
}

func (o *OrderService) Dequeue() (or []Order,err error){
	var q = queueUrl
	var l = linger
	result, err := o.Sqs.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl: &q,
		MaxNumberOfMessages: aws.Int64(10),
		WaitTimeSeconds: &l,
	})
	if err != nil {
		return nil,err
	}
	var os = make([]Order,0,10)
	for _,ms := range result.Messages {
		var order Order
		if err := json.Unmarshal([]byte(*ms.Body),&order); err != nil {
			return nil,err
		}
		order.ReceiptHandle = ms.ReceiptHandle
		os = append(os, order)
	}
	return os,nil
}

func split(all []Order,bucketSize int) [][]Order {
	a := make([][]Order,0)
	for index,order := range all {
		bucket := index / bucketSize
		if len(a) < bucket+1 {
			a = append(a,make([]Order,0))
		}
		a[bucket] = append(a[bucket],order)
	}
	return a
}


func (o *OrderService) Enqueue(or Order) error{
	var queueURL = queueUrl

	orders := make([]Order,0)
	for i := 0 ; i < or.NumberOfOrders ; i ++ {
		c := or
		c.Id = uuid.New().String()
		orders = append(orders,c)
	}

	splitted := split(orders,10)
	for _,bucket := range splitted {
		go func(b []Order){
			messages := make([]*sqs.SendMessageBatchRequestEntry,len(b))
			for i := 0 ; i < len(b) ; i ++ {
				arr,err := json.Marshal(b[i])
				if err != nil {
					fmt.Println("marshal : ",err)
					continue
				}
				var body = string(arr)
				copyIndex := strconv.Itoa(i)
				m := sqs.SendMessageBatchRequestEntry{
					MessageBody: &body,
					Id: &copyIndex,
				}
				messages[i] = &m
			}
			if _, err := o.Sqs.SendMessageBatch(&sqs.SendMessageBatchInput{
				Entries: messages,
				QueueUrl:    &queueURL,
			}); err != nil {
				fmt.Println("sqs : ",err)
			}
		}(bucket)
	}

	return nil
}

func (o *OrderService) AckQueue(handle *string) error {
	var q = queueUrl
	_, err := o.Sqs.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      &q,
		ReceiptHandle: handle,
	})
	if err != nil {
		return err
	}
	return nil
}



func (o *OrderService) Insert(or Order) error{
	_, err := o.Dbpool.Exec(context.Background(),
		"INSERT INTO orders(id, customerId, productId, orderDesc) VALUES($1, $2, $3, $4)",
		or.Id,or.CustomerId,or.ProductId,or.OrderDesc)

	if err != nil  {
		switch v := (err).(type) {
			case *pgconn.PgError:
				if v.Code == "23505" {
					return nil
				}
				return v
			default:
				fmt.Print("Error adding order to DB", err)
				return err
		}
	}
	return nil
}



