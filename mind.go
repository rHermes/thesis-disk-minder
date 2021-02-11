package main

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/rs/zerolog"
)

func main() {
	opts := func(w *zerolog.ConsoleWriter) {
		w.NoColor = true
		w.TimeFormat = time.Stamp
	}
	logger := zerolog.New(zerolog.NewConsoleWriter(opts)).
		With().Timestamp().Logger().Level(zerolog.InfoLevel)

	cfg := sarama.NewConfig()

}
