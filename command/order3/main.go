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
	"time"
	"github.com/ozgurrahmidonmez/taskpool/model"
	"github.com/ozgurrahmidonmez/taskpool/pool"
)

type Response struct {
	ErrorCode int    `json:"errorCode" binding:"required"`
	ErrorDesc string `json:"errorDesc" binding:"required"`
}

func (g *Response) Error() string{
	return "Order Failed " + strconv.Itoa(g.ErrorCode) + ":" + g.ErrorDesc
}

type Env struct {
	p *pool.Pool
	// Replace the reference to models.OrderService with an interface
	// describing its methods instead. All the other code remains exactly
	// the same.
	orders interface {
		Dequeue() (or []Order,err error)
		Enqueue(or Order) error
		AckQueue(handle *string) error
		Insert(or Order) error
	}
}

func (env *Env) poll() {
	f := func(o model.Data) {
		fmt.Println("consuming")
		consume(o.(Order),env)
	}
	p := pool.New(100,f,100000)
	env.p = &p
	for i := 0;i<10;i++{
		go func(){
			for {
				orders, err := env.orders.Dequeue()
				fmt.Println("dequeued : ",len(orders))
				if err != nil {
					fmt.Println("Can not poll", err)
					time.Sleep(1 * time.Second)
					continue
				}
				for _,o := range orders {
					p.Submit(o)
				}
			}
		}()
	}
}

func consume(o Order, env *Env){
	if err := env.orders.Insert(o); err != nil {
		fmt.Println(err)
		return
	}
	if err := env.orders.AckQueue(o.ReceiptHandle); err != nil {
		fmt.Println(err)
	}
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

func (env *Env) StopPoll(c *gin.Context) {
	(*env.p).Stop()
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
		orders: &OrderService{Dbpool: p,Sqs: s},
	}

	router := gin.Default()
	go env.poll()
	// profiler
	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()
	router.POST("/order",env.Add)
	router.POST("/poll/stop",env.StopPoll)
	e := router.Run("localhost:8080")
	if e != nil {
		os.Exit(1)
	}
}
