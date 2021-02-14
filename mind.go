package main

import (
	"fmt"
	"os"
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
	if time.Since(lastMsg.Timestamp) < (5 * time.Minute) {
		logger.Debug().Msg("Skipping as it's not been long enough since it was done")
		return nil
	}

	dur := lastMsg.Timestamp.Sub(firstMsg.Timestamp)

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

	if err := admin.DeleteTopic(topic); err != nil {
		return err
	}
	logger.Info().Msg("Removed topic")

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

func main() {

	opts := func(w *zerolog.ConsoleWriter) {
		w.NoColor = true
		w.TimeFormat = time.Stamp
	}
	logger := zerolog.New(zerolog.NewConsoleWriter(opts)).
		With().Timestamp().Logger().Level(LogLevel)

	logger.Debug().Msg("Started minding your business")

	cfg := sarama.NewConfig()
	cfg.Consumer.MaxWaitTime = 50 * time.Millisecond
	client, err := sarama.NewClient([]string{"localhost:9092"}, cfg)
	if err != nil {
		logger.Fatal().Err(err).Msg("Couldn't create client")
	}
	defer client.Close()

	if err := mloop(logger, client); err != nil {
		logger.Fatal().Err(err).Msg("Something went wrong in the main loop")
	}

	logger.Debug().Msg("Stopped minding your business")
}
