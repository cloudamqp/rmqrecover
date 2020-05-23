#!/bin/bash -eu

shards build --release --production --static
dir=rmqrecover-$(shards version)-static-$(uname -i)
rm -rf "$dir"
mkdir "$dir"
cp bin/rmqrecover README.md LICENSE "$dir"
strip "$dir/rmqrecover"
tar -zcvf "$dir.tar.gz" "$dir"
rm -rf "$dir"
