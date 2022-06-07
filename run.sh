#!/bin/sh
DIR=$(pwd)

docker run -v $DIR/data:/data -v $DIR/app:/app -v $DIR/result:/result docker.io/bitnami/spark:3 spark-submit /app/lastfm.py

