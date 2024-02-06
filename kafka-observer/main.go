package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/code-engine-go-sdk/codeenginev2"
	"github.com/IBM/go-sdk-core/v5/core"
	"github.com/IBM/sarama"
)

var (
	version = ""
	oldest  = true
	verbose = false
)

func main() {
	keepRunning := true
	log.Println("Starting a new Sarama consumer")

	if verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	version = sarama.DefaultVersion.String()
	version, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}

	config := sarama.NewConfig()
	config.Version = version
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.ClientID, _ = os.Hostname()
	config.Net.SASL.Enable = true
	config.Net.SASL.User = "token"
	config.Net.SASL.Password = os.Getenv("KAFKA_API_KEY")
	config.Net.TLS.Enable = true

	if oldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}

	brokers := []string{
		"broker-2-xqwbh5z6r9h5xz2d.kafka.svc10.us-south.eventstreams.cloud.ibm.com:9093",
		"broker-5-xqwbh5z6r9h5xz2d.kafka.svc10.us-south.eventstreams.cloud.ibm.com:9093",
		"broker-0-xqwbh5z6r9h5xz2d.kafka.svc10.us-south.eventstreams.cloud.ibm.com:9093",
		"broker-3-xqwbh5z6r9h5xz2d.kafka.svc10.us-south.eventstreams.cloud.ibm.com:9093",
		"broker-4-xqwbh5z6r9h5xz2d.kafka.svc10.us-south.eventstreams.cloud.ibm.com:9093",
		"broker-1-xqwbh5z6r9h5xz2d.kafka.svc10.us-south.eventstreams.cloud.ibm.com:9093",
	}

	consumer := Consumer{
		ready: make(chan bool),
	}

	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(brokers, "consuming-group-2", config)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}
	topics := []string{"encalada-topic"}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := client.Consume(ctx, topics, &consumer); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}
				log.Panicf("Error from consumer: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready
	log.Println("Sarama consumer up and running!...")

	sigusr1 := make(chan os.Signal, 1)
	signal.Notify(sigusr1, syscall.SIGUSR1)

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	for keepRunning {
		select {
		case <-ctx.Done():
			fmt.Print("hello done")
			log.Println("terminating: context cancelled")
			keepRunning = false
		case <-sigterm:
			fmt.Print("hello sigterm")
			log.Println("terminating: via signal")
			keepRunning = false
		case <-time.After(time.Second * 15):
			checkUnmarkesMessages(claim)
		}
	}
	cancel()
	wg.Wait()
	if err = client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
}

type Consumer struct {
	ready chan bool
}

func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}

func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	fmt.Println("Entering consumeClaim function")
	for {
		fmt.Println("Running for loop for consuming")
		select {
		case message, ok := <-claim.Messages(): //10 1A/1B/1A/1B  10A => success, 10B => failure
			if !ok {
				log.Printf("message channel was closed")
				return nil
			}

			// svc, err := getCodeengineService()
			// if err != nil {
			// 	panic("could not get the codeengine service")
			// }

			// projectID := "0c84faf7-e986-4da3-97d0-97e21c182e5f"
			// jobname := "my-job"
			// err = createJobrun(svc, projectID, jobname)
			// if err != nil {
			// 	panic("could not create the jobrun")
			// }

			log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s, partition = %v, offset = %v", string(message.Value), message.Timestamp, message.Topic, message.Partition, message.Offset)

			// fire jobrun creation

			// if jr creation == true then we session.MarkMessage(message, "")
			//session.MarkMessage(message, "")
			// session.Commit()

			// jobRunCreated := false
			// if jobRunCreated {
			// 	session.MarkMessage(message, "")
			// }
		case <-session.Context().Done():
			log.Printf("completed")
			return nil
		}
	}
}

func checkUnmarkesMessages(consumer sarama.ConsumerGroupClaim) {

	for i := 0; i < 10; i++ {
		message, ok := <-consumer.Messages()
		if !ok {
			log.Printf("message channel was closed")
		}

		log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s, partition = %v, offset = %v", string(message.Value), message.Timestamp, message.Topic, message.Partition, message.Offset)
	}

}

func getCodeengineService() (*codeenginev2.CodeEngineV2, error) {
	authenticator := &core.IamAuthenticator{
		ApiKey:       os.Getenv("CE_API_KEY"),
		ClientId:     "bx",
		ClientSecret: "bx",
		URL:          "https://iam.cloud.ibm.com",
	}

	codeEngineService, err := codeenginev2.NewCodeEngineV2(&codeenginev2.CodeEngineV2Options{
		Authenticator: authenticator,
		URL:           "https://api." + "au-syd" + ".codeengine.cloud.ibm.com/v2",
	})
	if err != nil {
		fmt.Printf("NewCodeEngineV2 error: %s\n", err.Error())
		return nil, err
	}
	return codeEngineService, nil
}

func createJobrun(codeEngineService *codeenginev2.CodeEngineV2, projectID, job string) error {
	createJobRunOptions := codeEngineService.NewCreateJobRunOptions(projectID)
	createJobRunOptions.SetJobName(job)
	createJobRunOptions.SetScaleArraySpec("0-1")

	_, _, err := codeEngineService.CreateJobRun(createJobRunOptions)
	if err != nil {
		return err
	}
	fmt.Println("Jobrun Created...")
	return nil
}

func commitMessage()
