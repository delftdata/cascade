import os
import subprocess
import time




# Define experiment parameters as a list of dictionaries
experiments = [
    {"mode": "thread", "taskmanagers": 16, "slots_per_tm": 1,  "benchmark_args": {"rps": 1000, "sec": 30, "threads": 10}},
    {"mode": "thread", "taskmanagers": 8, "slots_per_tm": 2,  "benchmark_args": {"rps": 1000, "sec": 30, "threads": 10}},
    {"mode": "thread", "taskmanagers": 4, "slots_per_tm": 4,   "benchmark_args": {"rps": 1000, "sec": 30, "threads": 10}},
    {"mode": "thread", "taskmanagers": 2, "slots_per_tm": 8,  "benchmark_args": {"rps": 1000, "sec": 30, "threads": 10}},
    {"mode": "thread", "taskmanagers": 1, "slots_per_tm": 16,  "benchmark_args": {"rps": 1000, "sec": 30, "threads": 10}},

    {"mode": "process", "taskmanagers": 16, "slots_per_tm": 1, "benchmark_args": {"rps": 1000, "sec": 30, "threads": 10}},
    {"mode": "process", "taskmanagers": 8, "slots_per_tm": 2, "benchmark_args": {"rps": 1000, "sec": 30, "threads": 10}},
    {"mode": "process", "taskmanagers": 4, "slots_per_tm": 4,  "benchmark_args": {"rps": 1000, "sec": 30, "threads": 10}},
    {"mode": "process", "taskmanagers": 2, "slots_per_tm": 8,  "benchmark_args": {"rps": 1000, "sec": 30, "threads": 10}},
    {"mode": "process", "taskmanagers": 1, "slots_per_tm": 16, "benchmark_args": {"rps": 1000, "sec": 30, "threads": 10}},
]




print("Tearing down docker containers")
subprocess.run(["docker", "compose", "down"], check=False)

for exp in experiments:
    print(f"Starting experiment {exp}")

    tm = exp["taskmanagers"]
    slots = exp["slots_per_tm"]
    parallelism = tm * slots
    
    # Start docker compose
    subprocess.run(["docker", "compose", "up", "-d", "--scale", f"taskmanager={tm}", "--force-recreate"], check=True, env={
        "TASK_SLOTS": str(slots)
    })

    time.sleep(10)
    
    # Run Flink job
    
    module = "deathstar_hotel_reservation.demo"
    if exp["mode"] == "process":
        module += "_process"

    flink_cmd = [
        "flink", "run", "--pyFiles", "/home/lvanmol/cascade/src,/home/lvanmol/cascade", 
        "--pyModule", module, "-d", "-p", str(parallelism)
    ]
    env = os.environ
    subprocess.run(flink_cmd, check=True, env=env)
    
    # Start benchmark
    filename = f"{tm}x{slots}_rps-{exp['benchmark_args']['rps']}_{exp['mode']}.csv"
    benchmark_cmd = [
        "python", "-u", "-m", "deathstar_hotel_reservation.start_benchmark", "--output", filename,
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
