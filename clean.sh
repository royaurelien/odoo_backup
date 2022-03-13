#!/bin/sh

docker-compose down -v
sudo rm -rf data/*
sudo rm -rf postgres/data/*
