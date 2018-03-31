package amqpw

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gobuffalo/buffalo/worker"
	"github.com/markbates/going/defaults"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

// Options are used to configure the AMQP Buffalo worker adapter.
type Options struct {
	// Connection is the AMQP connection to use.
	Connection *amqp.Connection
	// Logger is a logger interface to write the worker logs.
	Logger Logger
	// Name is used to identify the app as a consumer. Defaults to "buffalo".
	Name string
	// Exchange is used to customize the AMQP exchange name. Defaults to "".
	Exchange string
}

// ErrInvalidConnection is returned when the Connection opt is not defined.
var ErrInvalidConnection = errors.New("invalid connection")

// Ensures Adapter implements the buffalo.Worker interface.
var _ worker.Worker = &Adapter{}

// New creates a new AMQP adapter for Buffalo workers.
func New(opts Options) *Adapter {
	ctx := context.Background()

	opts.Name = defaults.String(opts.Name, "buffalo")

	if opts.Logger == nil {
		l := logrus.New()
		l.Level = logrus.InfoLevel
		l.Formatter = &logrus.TextFormatter{}
		opts.Logger = l
	}

	return &Adapter{
		Connection:   opts.Connection,
		Logger:       opts.Logger,
		consumerName: opts.Name,
		exchange:     opts.Exchange,
		ctx:          ctx,
	}
}

// Adapter implements the buffalo.Worker interface.
type Adapter struct {
	Connection   *amqp.Connection
	Channel      *amqp.Channel
	Logger       Logger
	consumerName string
	exchange     string
	ctx          context.Context
}

// Start connects to the broker.
func (q *Adapter) Start(ctx context.Context) error {
	q.Logger.Info("Starting AMQP Worker")
	q.ctx = ctx
	go func() {
		select {
		case <-ctx.Done():
			q.Stop()
		}
	}()
	// Ensure Connection is defined
	if q.Connection == nil {
		return ErrInvalidConnection
	}
	// Start new broker channel
	c, err := q.Connection.Channel()
	if err != nil {
		return err
	}
	q.Channel = c
	return nil
}

// Stop closes the connection to the broker.
func (q *Adapter) Stop() error {
	q.Logger.Info("Stopping AMQP Worker")
	return q.Channel.Close()
}

// Register consumes a task, using the declared worker.Handler
func (q *Adapter) Register(name string, h worker.Handler) error {
	msgs, err := q.Channel.Consume(
		name,
		q.consumerName,
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)

	if err != nil {
		return errors.WithStack(err)
	}

	go func() {
		for d := range msgs {
			q.Logger.Debugf("Received job %s: %s", name, d.Body)

			args := worker.Args{}
			err := json.Unmarshal(d.Body, &args)
			if err != nil {
				q.Logger.Errorf("Unable to retreive job %s args", name)
				continue
			}
			if err := h(args); err != nil {
				q.Logger.Errorf("Unable to process job %s", name)
				continue
			}
			if err := d.Ack(false); err != nil {
				q.Logger.Errorf("Unable to Ack job %s", name)
			}
		}
	}()

	return nil
}

// Perform enqueues a new job.
func (q Adapter) Perform(job worker.Job) error {
	q.Logger.Infof("Enqueuing job %s\n", job)

	err := q.Channel.Publish(
		q.exchange,  // exchange
		job.Handler, // routing key
		true,        // mandatory
		true,        // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         []byte(job.Args.String()),
		},
	)

	if err != nil {
		q.Logger.Errorf("error enqueuing job %s", job)
		return errors.WithStack(err)
	}
	return nil
}

// PerformIn performs a job delayed by the given duration.
func (q Adapter) PerformIn(job worker.Job, t time.Duration) error {
	q.Logger.Infof("Enqueuing job %s\n", job)
	d := int64(t / time.Second)

	// Trick broker using x-dead-letter feature:
	// the message will be pushed in a temp queue with the given duration as TTL.
	// When the TTL expires, the message is forwarded to the original queue.
	dq, err := q.Channel.QueueDeclare(
		fmt.Sprintf("%s_delayed_%d", job.Handler, d),
		false, // This is a temp queue
		true,  // Auto-deletion
		false,
		true,
		amqp.Table{
			"x-message-ttl":             d,
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": job.Handler,
		},
	)

	if err != nil {
		q.Logger.Errorf("error creating delayed temp queue for job %s", job)
		return errors.WithStack(err)
	}

	err = q.Channel.Publish(
		q.exchange, // exchange
		dq.Name,    // publish to temp delayed queue
		true,       // mandatory
		true,       // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         []byte(job.Args.String()),
		},
	)

	if err != nil {
		q.Logger.Errorf("error enqueuing job %s", job)
		return errors.WithStack(err)
	}
	return nil
}

// PerformAt performs a job at the given time.
func (q Adapter) PerformAt(job worker.Job, t time.Time) error {
	return q.PerformIn(job, t.Sub(time.Now()))
}
