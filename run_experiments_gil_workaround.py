import os
import subprocess
import time

args = {
    "messages_per_burst": 10,
    "sleeps_per_burst": 10,
    "sleep_time": 0.09,
    "seconds_per_burst": 1,
    "seconds": 100
}

def mps(num, producer_threads=1):
    return {
        "threads": producer_threads,
        "requests_per_second": num,
        "seconds": 50,
    }


# Define experiment parameters as a list of dictionaries
experiments = [
    # {"parallelism": 4, "benchmark_args": {**mps(20)}},
    # {"parallelism": 4, "benchmark_args": {**mps(40)}},
    # {"parallelism": 4, "benchmark_args": {**mps(60)}},
    # {"parallelism": 4, "benchmark_args": {**mps(80)}},
    # {"parallelism": 4, "benchmark_args": {**mps(100)}},

    # {"parallelism": 24, "benchmark_args": {**mps(200)}},
    # {"parallelism": 24, "benchmark_args": {**mps(400)}},
    # {"parallelism": 24, "benchmark_args": {**mps(600)}},
    # {"parallelism": 24, "benchmark_args": {**mps(800)}},
    # {"parallelism": 24, "benchmark_args": {**mps(200, producer_threads=10)}},
    # {"parallelism": 24, "benchmark_args": {**mps(400, producer_threads=10)}},
    # {"parallelism": 24, "benchmark_args": {**mps(600, producer_threads=20)}},
    {"parallelism": 24, "benchmark_args": {**mps(2000, producer_threads=20)}},
    # {"parallelism": 24, "benchmark_args": {**mps(2000, producer_threads=40)}},
    # {"parallelism": 24, "benchmark_args": {**mps(1000, threads=20)}},
]




print("Tearing down docker containers")
subprocess.run(["docker", "compose", "down"], check=False)

for e in ["baseline", "parallel"]:
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
            "--pyModule", "deathstar_movie_review.demo", "-d", "-p", str(exp['parallelism'])
        ]
        env = os.environ
        env["EXPERIMENT"] = e
        subprocess.run(flink_cmd, check=True, env=env)
        
        # Start benchmark
        filename = f"{e}_p-{exp['parallelism']}_mps-{exp['benchmark_args']['requests_per_second']}.pkl"
        benchmark_cmd = [
            "python", "-u", "-m", "deathstar_movie_review.start_benchmark", "--output", filename, "--experiment", e
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

print("All experiments completed.")
