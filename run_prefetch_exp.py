import os
import subprocess
import time

def rps(num, branch_chance, producer_threads=1):
    return {
        "threads": producer_threads,
        "requests_per_second": num,
        "seconds": 2,
        "branch_chance": branch_chance
    }


# Define experiment parameters as a list of dictionaries
experiments = [
    {"parallelism": 1, "benchmark_args": {**rps(1, 0.1, producer_threads=1)}},
    {"parallelism": 1, "benchmark_args": {**rps(1, 0.5, producer_threads=1)}},
    {"parallelism": 1, "benchmark_args": {**rps(1, 0.9, producer_threads=1)}},
]




print("Tearing down docker containers")
subprocess.run(["docker", "compose", "down"], check=False)

for e in ["prefetch"]:
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
        filename = f"{e}_p-{exp['parallelism']}_rps-{exp['benchmark_args']['requests_per_second']}_chance-{exp['benchmark_args']['branch_chance']}.pkl"
        benchmark_cmd = [
            "python", "-u", "-m", "deathstar_movie_review.start_prefetch_experiment", "--output", filename, "--experiment", e
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
