package amqpw

import (
	"context"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/gobuffalo/buffalo/worker"
	"github.com/markbates/going/randx"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/require"
)

var q *Adapter

// Setup the adapter
func TestMain(m *testing.M) {
	l := logrus.New()
	l.Level = logrus.InfoLevel
	l.Formatter = &logrus.TextFormatter{}

	var err error
	// Setup AMQP connection
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672")
	if err != nil {
		log.Fatal(err)
	}
	q = New(Options{
		Connection: conn,
		Name:       randx.String(20),
		Logger:     l,
	})

	ctx, cancel := context.WithCancel(context.Background())
	ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	go func() {
		select {
		case <-ctx.Done():
			cancel()
			l.Fatal(ctx.Err())
		}
	}()

	err = q.Start(ctx)
	if err != nil {
		cancel()
		l.Fatal(err)
	}

	code := m.Run()

	err = q.Stop()
	if err != nil {
		l.Fatal(err)
	}

	l.Info("Test stopped")

	os.Exit(code)
}

func Test_Perform(t *testing.T) {
	r := require.New(t)

	var hit bool
	wg := &sync.WaitGroup{}
	wg.Add(1)
	q.Register("perform", func(worker.Args) error {
		hit = true
		wg.Done()
		return nil
	})
	q.Perform(worker.Job{
		Handler: "perform",
	})
	wg.Wait()
	r.True(hit)
}

func Test_PerformAt(t *testing.T) {
	r := require.New(t)

	var hit bool
	wg := &sync.WaitGroup{}
	wg.Add(1)
	q.Register("perform_at", func(args worker.Args) error {
		hit = true
		wg.Done()
		return nil
	})
	q.PerformAt(worker.Job{
		Handler: "perform_at",
	}, time.Now().Add(5*time.Nanosecond))
	wg.Wait()
	r.True(hit)
}

func Test_PerformIn(t *testing.T) {
	r := require.New(t)

	var hit bool
	wg := &sync.WaitGroup{}
	wg.Add(1)
	q.Register("perform_in", func(worker.Args) error {
		hit = true
		wg.Done()
		return nil
	})
	q.PerformIn(worker.Job{
		Handler: "perform_in",
	}, 5*time.Nanosecond)
	wg.Wait()
	r.True(hit)
}
