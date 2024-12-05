# Cascade

## Development

### Testing

```
pytest
```

### Documentation

Documentation is done with [Google style docstrings](https://google.github.io/styleguide/pyguide.html) in the source code. 
It can be generated using Sphinx:

```
cd docs/
sphinx-apidoc -o source/ ../src
make html
```

and then opened at `docs/build/html/index.html`.

