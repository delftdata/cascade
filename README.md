# Cascade

## Benchmarking

Requirements:
- Docker
- Conda
- Local flink client

1. First create the conda environment with:

```
conda env create -f environment.yml
```

2. Activate the environment with:

```
conda activate cascade_env
```

3. Start the Kafka and Pyflink local clusters

```
docker compose up
```

This will launch:

- a Kafka broker at `localhost:9092` (`kafka:9093` for inter-docker communication!) and,
- a [Kafbat UI](https://github.com/kafbat/kafka-ui) at http://localhost:8080
- a local Flink cluster with `PyFlink` and all requirements, with a ui at http://localhost:8081

By default the flink cluster will run with 16 task slots. This can be changed
setting the `TASK_SLOTS` enviroment variable, for example:

```
TASK_SLOTS=32 docker compose up
```

You could also scale up the number of taskmanagers, each with the same defined
number of task slots (untested):

```
docker compose up --scale taskmanager=3
```

Once everything has started (for example, you can see the web UIs running), you 
can upload the benchmark job to the cluster. Note that the Kafka topics must be
emptied first, otherwise the job will immediately start consuming old events. 
You can use the Kafbat UI for this, for example by deleting topics or purging
messages. To start the job, first navigate to the cascade repo directory e.g. 
`cd /path/to/cascade`. Then run the following command, where `X` is the default 
parallelism desired:

```
flink run --pyFiles /path/to/cascade/src,/path/to/cascade --pyModule deathstar_movie_review.demo -p X
```

Once the job is submitted, you can start the benchmark. Open another terminal in
the same directory (and conda environment) and run:

```
python -m deathstar_movie_review.start_benchmark
```

This will start the benchmark by sending events to Kafka. The first phase will
initialise the state required for the benchmark, and is not measured. The second
phase starts the actual becnhmark.


### Notes

Currently trying to scale up higher than `-p 16`, however I ran into the 
following issue on `-p 64` with `TASK_SLOTS=128`, more configuration might be required? 

```
Caused by: java.io.IOException: Insufficient number of network buffers: required 65, but only 38 available. The total number of network buffers is currently set to 4096 of 32768 bytes each. You can increase this number by setting the configuration keys 'taskmanager.memory.network.fraction', 'taskmanager.memory.network.min', and 'taskmanager.memory.network.max'.
```


## Development

Cascade should work with Python 3.10 / 3.11 although other versions could work. Dependencies should first be installed with:

```
pip install -r requirements.txt
```

## (old) Testing

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

To view with debug logs:

```
pytest -m integration -s --log-level=DEBUG
```

## Documentation

Documentation can be generated using [pdoc](https://pdoc.dev/docs/pdoc.html):

```
pdoc --mermaid src/cascade
```


