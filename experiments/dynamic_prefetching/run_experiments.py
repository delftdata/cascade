import subprocess
import time


# Define experiment parameters as a list of dictionaries
experiments = [
    {"parallelism": 4, "benchmark_args": {"requests_per_second": 1000, "seconds": 30, "threads": 20, "experiment": "baseline", "chance": 0.9}},
    {"parallelism": 4, "benchmark_args": {"requests_per_second": 1000, "seconds": 30, "threads": 20, "experiment": "prefetch", "chance": 0.9}},
    {"parallelism": 4, "benchmark_args": {"requests_per_second": 1000, "seconds": 30, "threads": 20, "experiment": "baseline", "chance": 0.5}},
    {"parallelism": 4, "benchmark_args": {"requests_per_second": 1000, "seconds": 30, "threads": 20, "experiment": "prefetch", "chance": 0.5}},
    {"parallelism": 4, "benchmark_args": {"requests_per_second": 1000, "seconds": 30, "threads": 20, "experiment": "baseline", "chance": 0.1}},
    {"parallelism": 4, "benchmark_args": {"requests_per_second": 1000, "seconds": 30, "threads": 20, "experiment": "prefetch", "chance": 0.1}},
]




print("Tearing down docker containers")
subprocess.run(["docker", "compose", "down"], check=False)

for exp in experiments:
    print(f"Starting experiment {exp}")
    
    # Start docker compose
    subprocess.run(["docker", "compose", "up", "-d", "--scale", f"taskmanager={exp['parallelism']}", "--force-recreate"], check=True, env={
        "TASK_SLOTS": "1" 
    })

    time.sleep(10)
    
    # Run Flink job
    
    flink_cmd = [
        "flink", "run", "--pyFiles", "/home/lvanmol/cascade/src,/home/lvanmol/cascade", 
        "--pyModule", "experiments.dynamic_prefetching.submit_job", "-d", "-p", str(exp['parallelism'])
    ]
    subprocess.run(flink_cmd, check=True)

    # Start benchmark
    # filename = f"{e}_p-{exp['parallelism']}_mps-{exp['benchmark_args']['requests_per_second']}.pkl"
    benchmark_cmd = [
        "python", "-u", "-m", "experiments.dynamic_prefetching.run_prefetcher",
    ]

    for arg, val in exp['benchmark_args'].items():
        benchmark_cmd.append(f"--{arg}")
        benchmark_cmd.append(str(val))
    subprocess.run(benchmark_cmd, check=True)
    
    # Sleep for experiment duration
    # print(f"Sleeping for {exp['sleep']} seconds...")
    # time.sleep(exp['sleep'])
    
    # Stop docker compose
    subprocess.run(["docker", "compose", "down"], check=False)
    
    print(f"Experiment completed.")

