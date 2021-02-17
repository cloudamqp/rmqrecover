#!/bin/bash -eux

docker=$(which podman || which docker)
$docker build -t rmqrecover .
id=$($docker create rmqrecover)
$docker cp "$id":rmqrecover bin/
$docker rm "$id"

dir=rmqrecover-$(git describe)-static-$(uname -i)
rm -rf "$dir"
mkdir "$dir"
cp bin/rmqrecover README.md LICENSE "$dir"
strip "$dir/rmqrecover"
tar -zcvf "$dir.tar.gz" "$dir"
rm -rf "$dir"
