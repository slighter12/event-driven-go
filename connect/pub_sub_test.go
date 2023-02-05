package connect_test

import (
	"context"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/slighter12/event-driven-go/connect"
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

	sessions := connect.Redial(ctx, url)
	pubCh := make(chan []byte)

	wg := new(sync.WaitGroup)
	sub := func(message chan<- []byte) {
		connect.Subscribe(sessions, message)
	}

	for i := 0; i < subNum; i++ {
		go func(i int) {
			subCh := make(chan []byte)
			go sub(subCh)
			for v := range subCh {
				log.Debugf("Subscribe time: %v, message: %+v i:%v", time.Now(), string(v), i)
				wg.Done()
			}
		}(i)
	}
	time.Sleep(time.Second) //wait for subscribe connect success

	go func(message <-chan []byte) {
		connect.Publish(sessions, message)
	}(pubCh)

	wg.Add(1)
	for i := 0; i < messageNum; i++ {
		m := uuid.NewString()
		pubCh <- []byte(m)
		log.Debugf("Publish time: %v, message: %+v", time.Now(), m)
		wg.Add(subNum)
	}
	wg.Done()

	wg.Wait()
}

func BenchmarkPubSub(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sessions := connect.Redial(ctx, url)
	s := make(chan struct{})
	pubCh := make(chan []byte)

	go func(s <-chan struct{}) {
		subCh := make(chan []byte)
		go connect.Subscribe(sessions, subCh)
		for range subCh {
			<-s
		}
	}(s)

	time.Sleep(time.Second)

	go func(message <-chan []byte) {
		connect.Publish(sessions, message)
	}(pubCh)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pubCh <- []byte(strconv.FormatInt(int64(i), 32))
		s <- struct{}{}
	}
}
