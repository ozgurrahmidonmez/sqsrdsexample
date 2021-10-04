package orders

// func split(all []Order) [][]Order {

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"testing"
)


func TestSplit(t *testing.T) {
	orders := []Order{
		Order{"1",1,1,"order 1",nil,500},
		Order{"2",1,2,"order 1",nil,3500},
		Order{"3",1,2,"order 1",nil,100},
	}

	splitted := split(orders,10)
	if len(splitted) != 410 {
		t.Failed()
	}
}


type mockSQSSuccess struct {
	sqsiface.SQSAPI
}

type mockSQSUnmarshallError struct {
	sqsiface.SQSAPI
}

func (m mockSQSUnmarshallError) ReceiveMessage(*sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error){
	mockedBody1 := "{:\"1\",\n\t\"customerId\":1,\n\"productId\":1,\n\"orderDesc\": \"heyyo\",\n\"NumberOfOrders\": 500\n}"
	mockedBody2 := "{\"id\":\"2\",\n\t\"customerId\":1,\n\"productId\":1,\n\"orderDesc\": \"heyyo\",\n\"NumberOfOrders\": 500\n}"
	receiptHandle1 := "handle1"
	receiptHandle2 := "handle2"
	mockedMessage1 := sqs.Message{
		Body:   &mockedBody1,
		ReceiptHandle: &receiptHandle1,

	}
	mockedMessage2 := sqs.Message{
		Body:   &mockedBody2,
		ReceiptHandle: &receiptHandle2,
	}
	messages := []*sqs.Message{&mockedMessage1,&mockedMessage2}
	return &sqs.ReceiveMessageOutput{
		Messages: messages,
	},nil
}

func (m mockSQSSuccess) ReceiveMessage(*sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error){
	mockedBody1 := "{\"id\":\"1\",\n\t\"customerId\":1,\n\"productId\":1,\n\"orderDesc\": \"heyyo\",\n\"NumberOfOrders\": 500\n}"
	mockedBody2 := "{\"id\":\"2\",\n\t\"customerId\":1,\n\"productId\":1,\n\"orderDesc\": \"heyyo\",\n\"NumberOfOrders\": 500\n}"
	receiptHandle1 := "handle1"
	receiptHandle2 := "handle2"
	mockedMessage1 := sqs.Message{
		Body:   &mockedBody1,
		ReceiptHandle: &receiptHandle1,

	}
	mockedMessage2 := sqs.Message{
		Body:   &mockedBody2,
		ReceiptHandle: &receiptHandle2,
	}
	messages := []*sqs.Message{&mockedMessage1,&mockedMessage2}
	return &sqs.ReceiveMessageOutput{
		Messages: messages,
	},nil
}

func TestDequeueSuccess(t *testing.T) {
	mockOrderService := OrderService{
		nil,
		mockSQSSuccess{},
	}
	orders,err := mockOrderService.Dequeue()

	if len(orders) != 2 || err != nil {
		t.Failed()
	}

	if orders[0].ReceiptHandle == nil || orders[1].ReceiptHandle == nil {
		t.Failed()
	}

}

func TestDequeueFailsWithUnmarshallError(t *testing.T) {
	mockOrderService := OrderService{
		nil,
		mockSQSUnmarshallError{},
	}
	orders,err := mockOrderService.Dequeue()

	if len(orders) != 0 || err == nil {
		t.Failed()
	}

	switch err.(type) {
	case *json.SyntaxError:
	default:
		t.Failed()
	}


}

