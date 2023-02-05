package eventdriven_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	eventdriven "github.com/slighter12/event-driven-go"
)

const (
	url        = "amqp://root:root@localhost:5672"
	messageNum = 10
	subNum     = 5
	logLevel   = log.ErrorLevel
)

func TestMain(m *testing.M) {
	log.SetLevel(logLevel)
	exitCode := m.Run()
	os.Exit(exitCode)
}

func TestPubSub(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	eventdriven.Setup(ctx, url)

	a := &eventdriven.PubSub{
		Title: "aaa",
		Msg:   "csads",
		Subscribe: func(d []byte) {
			fmt.Println(d)
		},
	}
	eventdriven.Append(a)

	go eventdriven.Run()
	time.Sleep(time.Second)

	eventdriven.Publish(a)

	time.Sleep(time.Second)
}
