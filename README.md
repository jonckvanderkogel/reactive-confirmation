### Start up Kafka
Run the docker-compose (from the root of the project):
```
docker-compose up
```

### Listen for a new event
```
curl "http://localhost:8080/confirmation?id=121"
```

If this event does not arrive within the timeout the request will receive a default message.


### Adding a new confirmation to the Kafka stream
```
curl -i -H "Accept: application/json" -H "Content-Type: application/json" -X POST -d \
    '{
        "id": 120,
        "success": true,
        "status": "PENDING"
    }' http://localhost:8080/confirmation

curl -i -H "Accept: application/json" -H "Content-Type: application/json" -X POST -d \
    '{
        "id": 121,
        "success": true,
        "status": "ACKNOWLEDGED"
    }' http://localhost:8080/confirmation
```

