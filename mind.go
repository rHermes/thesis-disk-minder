package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/rs/zerolog"
)

const (
	// InputTopicPrefix is the prefix for input topics
	InputTopicPrefix = "INPUT_TOPIC_NAME_WITH_INDEX_SUFFIX_"

	// OutputTopicPrefix is the prefix for output topics
	OutputTopicPrefix = "TOPIC_NAME_WITH_INDEX_SUFFIX_"

	// LogLevel is the loglevel in the application
	LogLevel = zerolog.InfoLevel
)

var (
	// DeleteTopicsFlag indicates if we should create topics or not
	DeleteTopicsFlag = flag.Bool("delete-topics", false, "Should topics be deleted?")

	// CreateTopicsFlag indicates if we should create topics or not
	CreateTopicsFlag = flag.Bool("create-topics", false, "Should topics be created?")

	// WatchDiskSpaceFlag indicates if we should watch for diskspace
	WatchDiskSpaceFlag = flag.Bool("watch-disk-space", false, "Should we watch the disk space?")
)

func getRelevantTopics(logger zerolog.Logger, client sarama.Client) ([]string, error) {
	logger.Debug().Msg("Started looking for relevant topics")
	fStart := time.Now()
	topics, err := client.Topics()
	if err != nil {
		return nil, err
	}

	relevantTopics := make([]string, 0)
	for _, topic := range topics {
		if strings.HasPrefix(topic, OutputTopicPrefix) {
			relevantTopics = append(relevantTopics, topic)
		}
	}

	logger.Debug().Dur("dur", time.Since(fStart)).Msg("Done looking for relevant topics")
	return relevantTopics, nil
}

func mindTopic(logger zerolog.Logger, client sarama.Client, topic string) error {
	fStart := time.Now()
	defer func() {
		logger.Debug().Dur("dur", time.Since(fStart)).Msg("Done with mind topic")
	}()

	// We get the first and last to see
	first, err := client.GetOffset(topic, 0, sarama.OffsetOldest)
	if err != nil {
		return err
	}
	logger = logger.With().Int64("first", first).Logger()

	last, err := client.GetOffset(topic, 0, sarama.OffsetNewest)
	if err != nil {
		return err
	}
	logger = logger.With().Int64("last", last).Logger()

	if first == last {
		logger.Debug().Msg("Skipping as there are no messages")
		return nil
	}

	cons, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return err
	}
	defer cons.Close()

	pc, err := cons.ConsumePartition(topic, 0, last-1)
	if err != nil {
		return err
	}
	lastMsg := <-pc.Messages()

	if err := pc.Close(); err != nil {
		return nil
	}

	pc, err = cons.ConsumePartition(topic, 0, first)
	if err != nil {
		return err
	}
	firstMsg := <-pc.Messages()

	if err := pc.Close(); err != nil {
		return nil
	}

	// We check if the last message was more than 5 minutes ago. If so we take a guess that you are done
	if time.Since(lastMsg.Timestamp) < (2 * time.Minute) {
		logger.Debug().Msg("Skipping as it's not been long enough since it was done")
		return nil
	}

	dur := lastMsg.Timestamp.Sub(firstMsg.Timestamp)

	logger = logger.With().Dur("dur", dur).Logger()

	fd, err := os.Create("results/" + topic + ".txt")
	if err != nil {
		return err
	}
	defer fd.Close()

	fmt.Fprintf(fd, "%s,%d,%d,%d\n", topic, first, last, dur.Milliseconds())

	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		return err
	}

	if err := admin.DeleteRecords(topic, map[int32]int64{0: last}); err != nil {
		return err
	}
	logger.Info().Msg("Truncated topic")
	return nil
}

func mloop(logger zerolog.Logger, client sarama.Client) error {
	for {
		logger.Debug().Msg("Starting check round")
		topics, err := getRelevantTopics(logger, client)
		if err != nil {
			return err
		}
		for _, topic := range topics {
			logger := logger.With().Str("topic", topic).Logger()

			if err := mindTopic(logger, client, topic); err != nil {
				return err
			}

		}

		time.Sleep(30 * time.Second)
	}
}

func createDefaultTopics(logger zerolog.Logger, client sarama.Client) error {
	topicsSlice, err := client.Topics()
	if err != nil {
		return err
	}
	topicMap := makeStrLookupMap(topicsSlice)

	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		return err
	}

	for _, topic := range generateDefaultTopicNames() {
		logger := logger.With().Str("topic", topic).Logger()

		if _, ok := topicMap[topic]; ok {
			continue
		}

		if err := admin.CreateTopic(topic, &sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
			ConfigEntries: map[string]*string{
				"cleanup.policy":   strPtr("delete"),
				"compression.type": strPtr("snappy"),
				"segment.bytes":    strPtr(strconv.Itoa(1048576 * 50)),
			},
		}, false); err != nil {
			logger.Err(err).Msg("Couldn't create topic")
		} else {
			logger.Info().Msg("Created topic")
		}
	}

	return nil
}

func deleteDefaultTopics(logger zerolog.Logger, client sarama.Client) error {
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		return err
	}

	topics, err := client.Topics()
	if err != nil {
		return err
	}

	for _, topic := range topics {
		logger := logger.With().Str("topic", topic).Logger()

		if !strings.HasPrefix(topic, OutputTopicPrefix) {
			continue
		}

		if err := admin.DeleteTopic(topic); err != nil {
			logger.Err(err).Msg("Couldn't delete this topic")
		} else {
			logger.Info().Msg("Deleted topic")
		}
	}
	return nil
}

func main() {
	flag.Parse()

	opts := func(w *zerolog.ConsoleWriter) {
		w.NoColor = true
		w.TimeFormat = time.Stamp
	}
	logger := zerolog.New(zerolog.NewConsoleWriter(opts)).
		With().Timestamp().Logger().Level(LogLevel)

	cfg := sarama.NewConfig()
	cfg.Consumer.MaxWaitTime = 50 * time.Millisecond
	client, err := sarama.NewClient([]string{"localhost:9092"}, cfg)
	if err != nil {
		logger.Fatal().Err(err).Msg("Couldn't create client")
	}
	defer client.Close()

	if *DeleteTopicsFlag {
		logger.Info().Msg("deleting topics")
		if err := deleteDefaultTopics(logger, client); err != nil {
			logger.Fatal().Err(err).Msg("Couldn't delete topics")
		}
		logger.Info().Msg("deleted topics")

		if err := client.RefreshMetadata(); err != nil {
			logger.Fatal().Err(err).Msg("Couldn't refresh metadata")
		}
	}

	if *CreateTopicsFlag {
		logger.Info().Msg("Creating topics")
		if err := createDefaultTopics(logger, client); err != nil {
			logger.Fatal().Err(err).Msg("Couldn't create topics")
		}
		logger.Info().Msg("Created topics")

		if err := client.RefreshMetadata(); err != nil {
			logger.Fatal().Err(err).Msg("Couldn't refresh metadata")
		}
	}

	if *WatchDiskSpaceFlag {
		logger.Info().Msg("Started minding your business")
		if err := mloop(logger, client); err != nil {
			logger.Fatal().Err(err).Msg("Something went wrong in the main loop")
		}
		logger.Info().Msg("Stopped minding your business")
	}

}
