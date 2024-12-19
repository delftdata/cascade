# Cascade

## Development

Cascade should work with Python 3.10 / 3.11 although other versions could work. Dependencies should first be installed with:

```
pip install -r requirements.txt
```

## Testing

The `pip install` command should have installed a suitable version of `pytest`.

```
pytest --version
```

Depending on your python install you may have to run `python -m pytest` instead.

### Unit Tests

For unit tests, you can run the following command:

```
pytest
```


### Integration Tests

Integration tests require the Kafka service to be running. 
You can launch it with:

```
docker compose up -d
```

This will launch:
- a Kafka broker at localhost:9092 and,
- a [Kafbat UI](https://github.com/kafbat/kafka-ui) at http://localhost:8080

Before running integration tests, it's best to purge all existing messages.
This can be done in the Kafbat UI (under the `Topics` tab) or by fully 
restarting the container:

```
docker compose down
docker compose up -d
```

You should then be able to run integration tests with the following command:

```
pytest -m integration
```

## Documentation

Documentation can be generated using [pdoc](https://pdoc.dev/docs/pdoc.html):

```
pdoc --mermaid src/cascade
```


