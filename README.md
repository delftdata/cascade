# Cascade

## Development

### Testing

For unit tests:

```
pytest
```

Integration tests require Kafka to be running (with `docker compose up`):

```
pytest -m integration
```

To view with debug logs:

```
pytest -m integration -s --log-level=DEBUG
```


### Documentation

Documentation is done with [Google style docstrings](https://google.github.io/styleguide/pyguide.html) in the source code. 
It can be generated using [pdoc](https://pdoc.dev/docs/pdoc.html):

```
pdoc --mermaid src/cascade
```


