package main

import (
	"context"
	"flag"
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/google/go-cloud/internal/pubsub"
	"golang.org/x/sync/errgroup"

	"errors"
	"os"

	"log"

	raw "cloud.google.com/go/pubsub/apiv1"
	"github.com/google/go-cloud/gcp"
	"github.com/google/go-cloud/internal/pubsub/gcppubsub"
	"github.com/google/go-cloud/internal/pubsub/mempubsub"
	"github.com/google/go-cloud/internal/pubsub/rabbitpubsub"
	"github.com/streadway/amqp"
)

var n = flag.Int("n", 100, "number of messages to send and receive per goroutine")
var ng = flag.Int("ng", 100, "number of goroutines")
var env = flag.String("p", "mem", "pubsub provider to use [mem|gcp|rabbit]")
var topicName = flag.String("topic", "test-topic", "name of the topic to use")
var subName = flag.String("sub", "test-subscription-1", "name of the subscription to use")
var mmh = flag.Int("mmh", 3, "maximum for maxHandlers")
var verbose = flag.Bool("v", false, "whether to log verbosely")
var check = flag.Bool("check", false, "whether to check that all the sent messages were received")

func main() {
	flag.Parse()
	if err := bench(); err != nil {
		fmt.Fprintf(os.Stderr, "pubsubbench: %v\n", err)
		os.Exit(1)
	}
}

func bench() error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.AlignRight|tabwriter.Debug)
	fmt.Fprintln(w, "max handlers\tmsgs/sec sending\tmsgs/sec recv\tmsgs/sec round trip")
	for mh := 1; mh <= *mmh; mh++ {
		pubsub.MaxHandlers = mh
		var ctx = context.Background()
		top, sub, cleanup, err := openTopicAndSub(*env)
		if err != nil {
			return err
		}

		sendDt, err := timeFunc(func() error {
			if *verbose {
				log.Printf("Sending %d messages to topic %q", *n, *topicName)
			}
			var eg errgroup.Group
			for g := 0; g < *ng; g++ {
				eg.Go(func() error {
					for i := 1; i <= *n; i++ {
						bod := fmt.Sprintf("%d", i)
						m := &pubsub.Message{Body: []byte(bod)}
						if err := top.Send(ctx, m); err != nil {
							return fmt.Errorf("failed to send message %d of %d: %v", i, *n, err)
						}
					}
					return nil
				})
			}
			return eg.Wait()
		})
		if err != nil {
			return err
		}

		receiveDt, err := timeFunc(func() error {
			defer cleanup()
			if *verbose {
				log.Printf("Receiving %d messages", *n)
			}
			var eg errgroup.Group
			for g := 0; g < *ng; g++ {
				eg.Go(func() error {
					for i := 1; i <= *n; i++ {
						m, err := sub.Receive(ctx)
						if err != nil {
							return fmt.Errorf("failed to receive message %d of %d: %v", i, *n, err)
						}
						m.Ack()
					}
					return nil
				})
			}
			return eg.Wait()
		})
		if err != nil {
			return err
		}

		nm := float64(*n * *ng)
		smps := nm / sendDt.Seconds()
		rmps := nm / receiveDt.Seconds()
		rtmps := nm / (sendDt.Seconds() + receiveDt.Seconds())
		fmt.Fprintf(w, "%d\t%7.3f\t%7.3f\t%7.3f\n", mh, smps, rmps, rtmps)
	}
	w.Flush()
	return nil
}

// timeFunc times how long it takes to run the given function.
func timeFunc(f func() error) (time.Duration, error) {
	t0 := time.Now()
	err := f()
	dt := time.Now().Sub(t0)
	return dt, err
}

func openTopicAndSub(env string) (top *pubsub.Topic, sub *pubsub.Subscription, cleanup func(), err error) {
	var ctx = context.Background()
	switch env {
	case "mem":
		b := mempubsub.NewBroker([]string{*topicName})
		top = mempubsub.OpenTopic(b, *topicName)
		sub = mempubsub.OpenSubscription(b, *topicName, time.Second)
		cleanup = func() {
			top.Shutdown(ctx)
			sub.Shutdown(ctx)
		}
	case "gcp":
		var pubClient *raw.PublisherClient
		pubClient, err = raw.NewPublisherClient(ctx)
		if err != nil {
			err = fmt.Errorf("making publisher client: %v", err)
			return
		}
		var subClient *raw.SubscriberClient
		subClient, err = raw.NewSubscriberClient(ctx)
		if err != nil {
			err = fmt.Errorf("making subscriber client: %v", err)
			return
		}
		projIDEnv := os.Getenv("PROJECT_ID")
		if projIDEnv == "" {
			err = errors.New("$PROJECT_ID is not set")
			return
		}
		proj := gcp.ProjectID(projIDEnv)
		top = gcppubsub.OpenTopic(ctx, pubClient, proj, *topicName, nil)
		sub = gcppubsub.OpenSubscription(ctx, subClient, proj, *subName, nil)
		cleanup = func() {
			top.Shutdown(ctx)
			sub.Shutdown(ctx)
		}
	case "rabbit":
		top, sub, cleanup, err = openRabbit(*topicName, *subName)
		if err != nil {
			err = fmt.Errorf("starting rabbit: %v", err)
			return
		}
	}
	return
}

func openRabbit(topicName, subName string) (*pubsub.Topic, *pubsub.Subscription, func(), error) {
	const rabbitURL = "amqp://guest:guest@localhost:5672/"

	// Connect to RabbitMQ.
	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		return nil, nil, nil, err
	}

	// Declare an exchange and a queue. Bind the queue to the exchange.
	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, nil, err
	}
	exchangeName := topicName
	err = ch.ExchangeDeclare(
		exchangeName,
		"fanout", // kind
		false,    // durable
		false,    // delete when unused
		false,    // internal
		false,    // no-wait
		nil)      // args
	if err != nil {
		return nil, nil, nil, fmt.Errorf("declaring exchange: %v", err)
	}
	queueName := subName
	q, err := ch.QueueDeclare(
		queueName,
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil)   // arguments
	if err != nil {
		return nil, nil, nil, fmt.Errorf("declaring queue: %v", err)
	}
	err = ch.QueueBind(q.Name, q.Name, exchangeName,
		false, // no-wait
		nil)   // args
	if err != nil {
		return nil, nil, nil, fmt.Errorf("binding queue: %v", err)
	}
	ch.Close() // OpenSubscription will create its own channels.

	topic := rabbitpubsub.OpenTopic(conn, exchangeName)
	sub := rabbitpubsub.OpenSubscription(conn, queueName)
	cleanup := func() {
		ctx := context.Background()
		defer conn.Close()
		defer topic.Shutdown(ctx)
		defer sub.Shutdown(ctx)
	}
	return topic, sub, cleanup, nil
}
