package kafka

import (
	"log"
	"os"
	// "os/signal"
	// "syscall"
	"time"

	"github.com/pkg/errors"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"openreplay/backend/pkg/env"
	"openreplay/backend/pkg/queue/types"
)

type Message = kafka.Message

type Consumer struct {
	c              *kafka.Consumer
	messageHandler types.MessageHandler
	commitTicker   *time.Ticker
	pollTimeout    uint

	lastReceivedPrtTs map[int32]int64
}

func NewConsumer(
	group string,
	topics []string,
	messageHandler types.MessageHandler,
	autoCommit bool,
) *Consumer {
	protocol := "plaintext"
	if env.Bool("KAFKA_USE_SSL") {
		protocol = "ssl"
	}
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               env.String("KAFKA_SERVERS"),
		"group.id":                        group,
		"auto.offset.reset":               "earliest",
		"enable.auto.commit":              "false",
		"security.protocol":               protocol,
		"go.application.rebalance.enable": true,
		"max.poll.interval.ms":            env.Int("KAFKA_MAX_POLL_INTERVAL_MS"),
	})
	if err != nil {
		log.Fatalln(err)
	}
	subREx := "^("
	for i, t := range topics {
		if i != 0 {
			subREx += "|"
		}
		subREx += t
	}
	subREx += ")$"
	if err := c.Subscribe(subREx, nil); err != nil {
		log.Fatalln(err)
	}

	var commitTicker *time.Ticker
	if autoCommit {
		commitTicker = time.NewTicker(2 * time.Minute)
	}

	return &Consumer{
		c:                 c,
		messageHandler:    messageHandler,
		commitTicker:      commitTicker,
		pollTimeout:       200,
		lastReceivedPrtTs: make(map[int32]int64),
	}
}

func (consumer *Consumer) Commit() error {
	consumer.c.Commit() // TODO: return error if it is not "No offset stored"
	return nil
}

func (consumer *Consumer) commitAtTimestamps(
	getPartitionTime func(kafka.TopicPartition) (bool, int64),
	limitToCommitted bool,
) error {
	assigned, err := consumer.c.Assignment()
	if err != nil {
		return err
	}
	logPartitions("Actually assigned:", assigned)

	var timestamps []kafka.TopicPartition
	for _, p := range assigned { // p is a copy here since it is not a pointer
		shouldCommit, commitTs := getPartitionTime(p)
		if !shouldCommit {
			continue
		} // didn't receive anything yet
		p.Offset = kafka.Offset(commitTs)
		timestamps = append(timestamps, p)
	}
	offsets, err := consumer.c.OffsetsForTimes(timestamps, 2000)
	if err != nil {
		return errors.Wrap(err, "Kafka Consumer back commit error")
	}

	if limitToCommitted {
		// Limiting to already committed
		committed, err := consumer.c.Committed(assigned, 2000) // memorise?
		if err != nil {
			return errors.Wrap(err, "Kafka Consumer retrieving committed error")
		}
		logPartitions("Actually committed:", committed)
		for _, comm := range committed {
			if comm.Offset == kafka.OffsetStored ||
				comm.Offset == kafka.OffsetInvalid ||
				comm.Offset == kafka.OffsetBeginning ||
				comm.Offset == kafka.OffsetEnd {
				continue
			}
			for _, offs := range offsets {
				if offs.Partition == comm.Partition &&
					(comm.Topic != nil && offs.Topic != nil && *comm.Topic == *offs.Topic) &&
					comm.Offset > offs.Offset {
					offs.Offset = comm.Offset
				}
			}
		}
	}

	// TODO: check per-partition errors: offsets[i].Error
	_, err = consumer.c.CommitOffsets(offsets)
	return errors.Wrap(err, "Kafka Consumer back commit error")
}

func (consumer *Consumer) CommitBack(gap int64) error {
	return consumer.commitAtTimestamps(func(p kafka.TopicPartition) (bool, int64) {
		lastTs, ok := consumer.lastReceivedPrtTs[p.Partition]
		if !ok {
			return false, 0
		}
		return true, lastTs - gap
	}, true)
}

func (consumer *Consumer) CommitAtTimestamp(commitTs int64) error {
	return consumer.commitAtTimestamps(func(p kafka.TopicPartition) (bool, int64) {
		return true, commitTs
	}, false)
}

func (consumer *Consumer) ConsumeNext() error {
	ev := consumer.c.Poll(int(consumer.pollTimeout))
	if ev == nil {
		return nil
	}

	if consumer.commitTicker != nil {
		select {
		case <-consumer.commitTicker.C:
			consumer.Commit()
		default:
		}
	}

	switch e := ev.(type) {
	case *kafka.Message:
		if e.TopicPartition.Error != nil {
			return errors.Wrap(e.TopicPartition.Error, "Consumer Partition Error")
		}
		ts := e.Timestamp.UnixMilli()
		consumer.messageHandler(decodeKey(e.Key), e.Value, &types.Meta{
			Topic:     *(e.TopicPartition.Topic),
			ID:        uint64(e.TopicPartition.Offset),
			Timestamp: ts,
		})
		consumer.lastReceivedPrtTs[e.TopicPartition.Partition] = ts
	// case kafka.AssignedPartitions:
	// 	logPartitions("Kafka Consumer: Partitions Assigned", e.Partitions)
	// 	consumer.partitions = e.Partitions
	// 	consumer.c.Assign(e.Partitions)
	// 	log.Printf("Actually partitions assigned!")
	// case kafka.RevokedPartitions:
	// 	log.Println("Kafka Cosumer: Partitions Revoked")
	// 	consumer.partitions = nil
	// 	consumer.c.Unassign()
	case kafka.Error:
		if e.Code() == kafka.ErrAllBrokersDown || e.Code() == kafka.ErrMaxPollExceeded {
			os.Exit(1)
		}
		log.Printf("Consumer error: %v\n", e)
	}
	return nil
}

func (consumer *Consumer) Close() {
	if consumer.commitTicker != nil {
		consumer.Commit()
	}
	if err := consumer.c.Close(); err != nil {
		log.Printf("Kafka consumer close error: %v", err)
	}
}

// func (consumer *Consumer) consume(
// 	message func(m *kafka.Message) error,
// 	commit func(c *kafka.Consumer) error,
// ) error {
// 	if err := consumer.c.Subscribe(consumer.topic, nil); err != nil {
// 		return err
// 	}
// 	defer consumer.close()
// 	sigchan := make(chan os.Signal, 1)
// 	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
// 	ticker := time.NewTicker(consumer.commitInterval)
// 	defer ticker.Stop()
// 	for {
// 		select {
// 		case <-sigchan:
// 			return commit(consumer.c)
// 		case <-ticker.C:
// 			if err := commit(consumer.c); err != nil {
// 				return err
// 			}
// 		default:
// 			ev := consumer.c.Poll(consumer.pollTimeout)
// 			if ev == nil {
// 				continue
// 			}
// 			switch e := ev.(type) {
// 			case *kafka.Message:
// 				if e.TopicPartition.Error != nil {
// 					log.Println(e.TopicPartition.Error)
// 					continue
// 				}
// 				if err := message(e); err != nil {
// 					return err
// 				}
// 			case kafka.AssignedPartitions:
// 				if err := consumer.c.Assign(e.Partitions); err != nil {
// 					return err
// 				}
// 			case kafka.RevokedPartitions:
// 				if err := commit(consumer.c); err != nil {
// 					return err
// 				}
// 				if err := consumer.c.Unassign(); err != nil {
// 					return err
// 				}
// 			case kafka.Error:
// 				log.Println(e)
// 				if e.Code() == kafka.ErrAllBrokersDown {
// 					return e
// 				}
// 			}
// 		}
// 	}
// }

// func (consumer *Consumer) Consume(
// 	message func(key uint64, value []byte) error,
// ) error {
// 	return consumer.consume(
// 		func(m *kafka.Message) error {
// 			return message(decodeKey(m.Key), m.Value)
// 		},
// 		func(c *kafka.Consumer) error {
// 			if _, err := c.Commit(); err != nil {
// 				log.Println(err)
// 			}
// 			return nil
// 		},
// 	)
// }

// func (consumer *Consumer) ConsumeWithCommitHook(
// 	message func(key uint64, value []byte) error,
// 	commit func() error,
// ) error {
// 	return consumer.consume(
// 		func(m *kafka.Message) error {
// 			return message(decodeKey(m.Key), m.Value)
// 		},
// 		func(c *kafka.Consumer) error {
// 			if err := commit(); err != nil {
// 				return err
// 			}
// 			if _, err := c.Commit(); err != nil {
// 				log.Println(err)
// 			}
// 			return nil
// 		},
// 	)
// }
