import os
import subprocess
import time

args = {
    "messages_per_burst": 10,
    "sleeps_per_burst": 10,
    "sleep_time": 0.08,
    "seconds_per_burst": 1,
    "bursts": 100
}

mps_20 = { 
    **args, 
    "messages_per_burst": 20,
    "sleeps_per_burst": 20,
    "sleep_time": 0.08/2,
}

mps_50 = { 
    **args, 
    "messages_per_burst": 50,
    "sleeps_per_burst": 50,
    "sleep_time": 0.08/5,
}

# Define experiment parameters as a list of dictionaries
experiments = [
    {"parallelism": 16, "benchmark_args": {**args}},
    {"parallelism": 16, "benchmark_args": {**mps_20}},
    {"parallelism": 16, "benchmark_args": {**mps_50}},

    {"parallelism": 8, "benchmark_args": {**args}},
    {"parallelism": 8, "benchmark_args": {**mps_20}},

    {"parallelism": 4, "benchmark_args": {**mps_20}},
    {"parallelism": 4, "benchmark_args": {**args}},

    {"parallelism": 2, "benchmark_args": {**args}},
    {"parallelism": 2, "benchmark_args": {**mps_20}},

    {"parallelism": 1, "benchmark_args": {**args}},
    {"parallelism": 1, "benchmark_args": {**mps_20}},

    {"parallelism": 8, "benchmark_args": {**mps_50}},
    {"parallelism": 4, "benchmark_args": {**mps_50}},
    {"parallelism": 2, "benchmark_args": {**mps_50}},
    {"parallelism": 1, "benchmark_args": {**mps_50}},
]




print("Tearing down docker containers")
subprocess.run(["docker", "compose", "down"], check=True)

for e in ["parallel", "base", "piplined"]:
    for exp in experiments:
        print(f"Starting experiment {exp}")
        
        # Start docker compose
        subprocess.run(["docker", "compose", "up", "-d"], check=True)

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
        filename = f"{e}_p-{exp['parallelism']}_mps-{exp['benchmark_args']['messages_per_burst']}.plk"
        benchmark_cmd = [
            "python", "-u", "-m", "deathstar_movie_review.start_benchmark", "--output", filename
        ]

        for arg, val in exp['benchmark_args'].items():
            benchmark_cmd.append(f"--{arg}")
            benchmark_cmd.append(str(val))
        subprocess.run(benchmark_cmd, check=True)
        
        # Sleep for experiment duration
        # print(f"Sleeping for {exp['sleep']} seconds...")
        # time.sleep(exp['sleep'])
        
        # Stop docker compose
        subprocess.run(["docker", "compose", "down"], check=True)
        
        print(f"Experiment completed.")

print("All experiments completed.")
