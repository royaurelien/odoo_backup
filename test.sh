#!/bin/sh

# curl http://localhost:8004/dump -H "Content-Type: application/json" --data '{"name": "esra"}'
curl http://localhost:8004/restore -H "Content-Type: application/json" --data '{"name": "test1"}'
