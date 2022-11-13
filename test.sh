#!/bin/sh


# curl http://localhost:8004/restore -H "Content-Type: application/json" --data '{"name": "base3", "filename": "TOILESCHICS-PROD_20220124_0929.zip"}'
# curl http://localhost:8004/dump -H "Content-Type: application/json" --data '{"name": "base1"}'

# curl http://localhost:8004/restore -H "Content-Type: application/json" --data '{"name": "moineaux", "filename": "backup.zip"}'
curl http://localhost:8004/dump -H "Content-Type: application/json" --data '{"name": "moineaux"}'
