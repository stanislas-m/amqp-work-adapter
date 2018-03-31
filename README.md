AMQP worker adapter for Buffalo
===============================

This package implements the `github.com/gobuffalo/buffalo/worker.Worker` interface using the [`github.com/streadway/amqp`](https://github.com/streadway/amqp) package.

It allows AMQP-compatible message brokers, such as [RabbitMQ](https://www.rabbitmq.com), to process [Buffalo's background tasks](https://gobuffalo.io/en/docs/workers).

## Setup

```go
import "github.com/stanislas-m/amqp-work-adapter"
import "github.com/streadway/amqp"

// ...

conn, err := amqp.Dial("amqp://guest:guest@localhost:5672")
if err != nil {
    log.Fatal(err)
}

buffalo.New(buffalo.Options{
  // ...
  Worker: amqpw.New(amqpw.Options{
    Connection: conn,
    Name:           "myapp",
    MaxConcurrency: 25,
  }),
  // ...
})
```