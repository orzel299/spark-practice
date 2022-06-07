#!/bin/sh
DIR=$(pwd)

docker run -v $DIR/data:/data -v $DIR/app:/app -v $DIR/result:/result docker.io/bitnami/spark:3 spark-submit /app/lastfm.py

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