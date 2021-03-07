package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
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

	// TempTopic is a temporary topic, that is used to test continues streaming
	TempTopic = "TEMP_TOPIC_HERE"

	// LogLevel is the loglevel in the application
	LogLevel = zerolog.InfoLevel
)

var (
	// DeleteTopicsFlag indicates if we should delete topics or not
	DeleteTopicsFlag = flag.Bool("delete-topics", false, "Should topics be deleted?")

	// CreateTopicsFlag indicates if we should create topics or not
	CreateTopicsFlag = flag.Bool("create-topics", false, "Should topics be created?")

	// DeleteTempTopicFlag indicates if we should delete the temporary topic
	DeleteTempTopicFlag = flag.Bool("delete-temp-topic", false, "Should temporary topic be deleted?")

	// CreateTempTopicFlag indicates if we should create topics or not
	CreateTempTopicFlag = flag.Bool("create-temp-topic", false, "Should the temporary topic be created?")

	// WatchDiskSpaceFlag indicates if we should watch for diskspace
	WatchDiskSpaceFlag = flag.Bool("watch-disk-space", false, "Should we watch the disk space?")

	// TempTopicSenderFlag indicates if we should start the temp topic sending loop
	TempTopicSenderFlag = flag.Bool("temp-topic-sender", false, "Should we start the temp topic sender loop")
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

func createTemporaryTopic(logger zerolog.Logger, client sarama.Client) error {
	topicsSlice, err := client.Topics()
	if err != nil {
		return err
	}
	topicMap := makeStrLookupMap(topicsSlice)

	if _, ok := topicMap[TempTopic]; ok {
		logger.Info().Msg("Topic exists, not creating")
		return nil
	}

	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		return err
	}

	der := time.Second * 30

	if err := admin.CreateTopic(TempTopic, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
		ConfigEntries: map[string]*string{
			"cleanup.policy":   strPtr("delete"),
			"compression.type": strPtr("snappy"),
			"segment.bytes":    strPtr(strconv.Itoa(1048576 * 50)),
			"retention.ms":     strPtr(strconv.FormatInt(der.Milliseconds(), 10)),
		},
	}, false); err != nil {
		logger.Err(err).Msg("Couldn't create topic")
		return err
	}

	logger.Info().Msg("Temp topic created")
	return nil
}

func deleteTemporaryTopic(logger zerolog.Logger, client sarama.Client) error {
	topicsSlice, err := client.Topics()
	if err != nil {
		return err
	}

	topicMap := makeStrLookupMap(topicsSlice)

	if _, ok := topicMap[TempTopic]; !ok {
		logger.Info().Msg("Temp topic doesn't exist")
		return nil
	}

	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		return err
	}

	if err := admin.DeleteTopic(TempTopic); err != nil {
		logger.Err(err).Msg("Couldn't delete temp topic")
		return err
	}

	logger.Info().Msg("Deleted temp topic")
	return nil
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

// tempTopicLoop sends data to a topic in an increasing wave
func tempTopicLoop(logger zerolog.Logger, client sarama.Client) error {
	ap, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		return err
	}
	defer ap.Close()

	// Ticker when we increase the rate
	incTic := time.NewTicker(time.Second * 5)
	defer incTic.Stop()

	rps := uint64(600)
	rpsInc := uint64(500)

	rps = uint64(5_000_000)

	// Ticker which we
	sendTicker := time.NewTicker(time.Second / time.Duration(rps))
	defer sendTicker.Stop()

	statusTicker := time.NewTicker(time.Second * 2)
	defer statusTicker.Stop()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	ll := logger.Sample(zerolog.Sometimes)
	ll = ll

	for {
		select {
		case <-statusTicker.C:
			// logger.Info().Msg("We are here")
		case <-incTic.C:
			ap.Input() <- &sarama.ProducerMessage{
				Topic: TempTopic,
				Value: sarama.StringEncoder("print this please"),
			}

			continue

			rps += rpsInc
			logger.Info().Uint64("rps", rps).Msg("Increasing rate")
			sendTicker.Reset(time.Second / time.Duration(rps))

		case a := <-c:
			logger.Info().Str("signal", a.String()).Msg("We got a signal, quiting")
			return nil

		case <-sendTicker.C:
			// ll.Info().Msg("We sending")

			ap.Input() <- &sarama.ProducerMessage{
				Topic: TempTopic,
				Value: sarama.StringEncoder("We teste this"),
			}

		case succ := <-ap.Successes():
			succ = succ
			logger.Info().Msg("We got success")

		case err := <-ap.Errors():
			logger.Err(err).Msg("Something went wrong")
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

	if *DeleteTempTopicFlag {
		logger.Info().Msg("deleting temp topic")
		if err := deleteTemporaryTopic(logger, client); err != nil {
			logger.Fatal().Err(err).Msg("Couldn't delete temp topic")
		}

		if err := client.RefreshMetadata(); err != nil {
			logger.Fatal().Err(err).Msg("Couldn't refresh metadata")
		}
	}

	if *CreateTempTopicFlag {
		logger.Info().Msg("creating temp topic")
		if err := createTemporaryTopic(logger, client); err != nil {
			logger.Fatal().Err(err).Msg("Couldn't create temp topic")
		}

		if err := client.RefreshMetadata(); err != nil {
			logger.Fatal().Err(err).Msg("Couldn't refresh metadata")
		}
	}

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

	if *TempTopicSenderFlag {
		logger.Info().Msg("Starting temp topic sender")
		if err := tempTopicLoop(logger, client); err != nil {
			logger.Fatal().Err(err).Msg("Something went wrong in the tempTopicLoop")
		}
		logger.Info().Msg("Stopped temp topic sender")
	}

}
