# RMQ Recover

Recovers messages from a RabbitMQ mnesia directory.
It can both report number of messages in all files in a mnesia directory,
but can also republish them to a new cluster.

## Known issues

It cannot yet extract message properties or headers, only vhost, exchange,
routing key and body.

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
Usage: rmqrecover DIRECTORY [ report | republish URI ]
```

## Contributing

1. Fork it (<https://github.com/cloudamqp/rmqrecover/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Contributors

- [Carl Hörberg](https://github.com/carlhoerberg) - creator and maintainer
