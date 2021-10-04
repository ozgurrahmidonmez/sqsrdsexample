package main

//https://blog.depa.do/post/gin-validation-errors-handling
//https://golang.org/doc/tutorial/database-access
//https://go.dev/blog/pipelines

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/gin-gonic/gin"
	_ "github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/jackc/puddle"
	"log"
	"net/http"
	_ "net/http/pprof"
	. "order/orders"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"
)

const minParallelism = 100
const maxParalellism = 200


type Response struct {
	ErrorCode int    `json:"errorCode" binding:"required"`
	ErrorDesc string `json:"errorDesc" binding:"required"`
}

func (g *Response) Error() string{
	return "Order Failed " + strconv.Itoa(g.ErrorCode) + ":" + g.ErrorDesc
}

type Env struct {
	// Replace the reference to models.OrderService with an interface
	// describing its methods instead. All the other code remains exactly
	// the same.
	parallelism int64
	orders interface {
		Dequeue() (or []Order,err error)
		Enqueue(or Order) error
		AckQueue(handle *string) error
		Insert(or Order) error
	}
}

// long polling -- ReceiveMessageWaitTimeSeconds = 20 sec
func (env *Env) poll(done <- chan struct{}) <- chan Order{
	queue := make(chan Order)
	f := func(in chan <- Order) {
		for{
			orders,err := env.orders.Dequeue()
			if err != nil{
				fmt.Println("Can not poll",err)
				time.Sleep(1 * time.Second)
				continue
			}
			asChannel := make(chan Order,len(orders) * 2)
			for _,r := range orders {
				asChannel <- r
			}

			for {
				if len(asChannel) > 0 {
					r := <-asChannel
					select {
						case in <- r:
						case <-time.After(time.Millisecond * 1000):
						// add consumer
							current := env.parallelism
							if current < maxParalellism && atomic.CompareAndSwapInt64(&env.parallelism,current,current+1) {
								fmt.Println("Adding more parallelism, current : ",current)
								go env.consume(done,queue)
								// not let the order dropped
							asChannel <- r
							break
						}
						//fmt.Println("Max parallelism is reached")
					case <- done:
						return
					}
					continue
				}
				break
			}
		}
	}
	for i := 0; i < 100; i++ {
		go f(queue)
	}
	return queue
}

func (env *Env)  consume(done <- chan struct{},queue <- chan Order){
	go func() {
		for {
			select {
			case or := <- queue:
				start := time.Now().Unix()
				if err := env.orders.Insert(or); err != nil {
					fmt.Println(err)
					continue
				}
				fmt.Println("Push time : ",time.Now().Unix() - start)
				start2 := time.Now().Unix()
				if err := env.orders.AckQueue(or.ReceiptHandle); err != nil {
					fmt.Println(err)
					continue
				}
				fmt.Println("Ack : ",time.Now().Unix() - start2)
			case <-time.After(time.Millisecond * 1000):
				// remove this consumer
				current := env.parallelism
				if current > minParallelism && atomic.CompareAndSwapInt64(&env.parallelism,current,current-1) {
					fmt.Println("Removing  parallelism, current : ",current)
					return
				}
			case <-done:
				return
			}
		}
	}()
}


func (env *Env) Add(c *gin.Context) {
	var order Order
	if err := c.BindJSON(&order);err != nil {
		fmt.Println(err)
		c.JSON(http.StatusBadRequest, Response{1,"bind exception when adding order"})
		return
	}
	err := env.orders.Enqueue(order)
	if err != nil {
		c.JSON(http.StatusBadRequest, Response{2,"can not enqueue"})
		return
	}
	c.JSON(http.StatusOK,Response{0,"ok"})
	return
}

func main() {
	fmt.Println(runtime.GOMAXPROCS(8))
	sess, err := session.NewSessionWithOptions(session.Options{
		Config: aws.Config{Region: aws.String("eu-west-1")},
		Profile: "default",
	})

	s := sqs.New(sess)

	//p, err := pgxpool.Connect(context.Background(), "postgresql://postgres:ozgotozgot1@database-1.cptj1r7jikob.eu-west-1.rds.amazonaws.com:5432")
	p, err := pgxpool.Connect(context.Background(), "postgresql://postgres:postgres@localhost:5432")
	if err != nil {
		if _, err := fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err); err != nil {
			return
		}
		os.Exit(1)
	}
	defer p.Close()

	env := &Env{
		parallelism: minParallelism,orders: &OrderService{Dbpool: p,Sqs: s},
	}

	router := gin.Default()
	done := make(chan struct{})
	queue := env.poll(done)
	// add paralelism
	for i := 0; i < int(env.parallelism); i++ {
		env.consume(done,queue)
	}
	// profiler
	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()
	router.POST("/order",env.Add)
	e := router.Run("localhost:8080")
	if e != nil {
		os.Exit(1)
	}
}