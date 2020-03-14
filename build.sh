#!/bin/bash -eu

shards build --release --static
strip -s bin/rmqrecover
mv bin/rmqrecover bin/rmqrecover-$(shards version)-static-$(uname -i)
