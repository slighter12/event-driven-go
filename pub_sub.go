package eventdriven

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/slighter12/event-driven-go/connect"
)

type PubSubRegistry interface {
	Subscribe(data []byte) func()
}

type PubSub struct {
	Title     string
	Msg       any
	Subscribe func([]byte)
}

type pubsub struct {
	member map[string][]*PubSub

	sync.Once
	mu   sync.Mutex
	pub  chan []byte
	sub  chan []byte
	conn chan chan connect.Session
}

var defaultpubsub *pubsub

func Publish(data *PubSub) error {
	if defaultpubsub == nil {
		panic("should init defaultpubsub memery")
	}
	return defaultpubsub.Publish(data)
}

func (p *pubsub) Publish(data *PubSub) error {
	p.Do(func() {
		go connect.Publish(p.conn, p.pub)
	})
	tmp := struct {
		Title string
		Msg   any
	}{
		Title: data.Title,
		Msg:   data.Msg,
	}
	d, err := json.Marshal(tmp)
	if err != nil {
		return err
	}
	p.pub <- d
	return nil
}

func Setup(ctx context.Context, url string) {
	pub := make(chan []byte)
	sub := make(chan []byte)
	sessions := connect.Redial(ctx, url)

	defaultpubsub = &pubsub{
		member: make(map[string][]*PubSub),
		pub:    pub,
		sub:    sub,
		conn:   sessions,
	}
}

func Run() {
	if defaultpubsub == nil {
		panic("should init defaultpubsub memery")
	}
	defaultpubsub.Run()
}

func (p *pubsub) Run() {
	go connect.Subscribe(p.conn, p.sub)

	for v := range p.sub {
		tmp := struct {
			Title string
			Msg   json.RawMessage
		}{}

		json.Unmarshal(v, &tmp)

		if subs, ok := p.member[tmp.Title]; ok {
			for _, sub := range subs {
				sub.Subscribe(tmp.Msg)
			}
		}
	}
}

func Append(mem ...*PubSub) {
	defaultpubsub.mu.Lock()
	defer defaultpubsub.mu.Unlock()
	for _, v := range mem {
		defaultpubsub.member[v.Title] = append(defaultpubsub.member[v.Title], v)
	}
}
