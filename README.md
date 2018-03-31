AMQP worker adapter for Buffalo
===============================

This package implements the `github.com/gobuffalo/buffalo/worker.Worker` interface using the [`github.com/streadway/amqp`](https://github.com/streadway/amqp) package.

It allows AMQP-compatible message brokers, such as [RabbitMQ](https://www.rabbitmq.com), to process [Buffalo's background tasks](https://gobuffalo.io/en/docs/workers).

## Requirements
* A RabbitMQ broker (other AMQP brokers will work for non-delayed tasks)
* For RabbitMQ delayed tasks: [Delayed message exchange plugin](https://github.com/rabbitmq/rabbitmq-delayed-message-exchange)

## Setup

TODO!