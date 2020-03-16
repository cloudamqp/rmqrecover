# RMQ Recover

Recovers messages from a RabbitMQ mnesia directory.
It can both report number of messages in all files in a mnesia directory,
but can also republish them to a new cluster. It extracts vhost, exchange,
routing key, properties (including headers) and the body.

## Known issues

By default RabbitMQ embeds messages smaller than 4KB into the queue index.
A message that is enqueued in multiple queues are therefor reported/
republished multiple times. If you republish you can end up with duplicates.

## Installation

Download: <https://github.com/cloudamqp/rmqrecover/releases>

From source:

```bash
git clone git@github.com:cloudamqp/rmqrecover.git
cd rmqrecover
shards build --release
```

## Usage

```
Usage: rmqrecover [ arguments ]
  -D DIR, --directory=DIR          mnesia directory to scan
  -m MODE, --mode=MODE             report (default) or republish
  -u NAME, --uri=URI               AMQP URI to republish to
  -v, --verbose                    Hexdump while reading
  --version                        Show version
  -h, --help                       Show this help
```

## Contributing

1. Fork it (<https://github.com/cloudamqp/rmqrecover/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Contributors

- [Carl Hörberg](https://github.com/carlhoerberg) - creator and maintainer
