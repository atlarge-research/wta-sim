#!/usr/bin/env python3.7
import itertools
import os
import subprocess

job_directory = "jobscripts"
os.makedirs(job_directory, exist_ok=True)

trace_dir = "/var/scratch/lvs215/WTA/parquet/"
output_location = "/var/scratch/lvs215/ic2e-wta-output/simulation_output/"
slack_location = "/var/scratch/lvs215/ic2e-wta-output/look_ahead/"

machine_resources = [128, 12]
machine_tdps = [280, 95]
machine_base_clocks = [2.9, 4.1]
machine_fractions = [0.5, 0.5]

# Variations to try:
target_utilizations = [0.3]
task_selection_policies = ["fcfs"]
task_placement_policies = ["look_ahead", "fastest_machine"]
dvfs_enabled = [True, False]

subprocess.run("mvn package", shell=True)

for folder in next(os.walk(trace_dir))[1]:
    if folder == "alibaba_from_flat":
        continue  # Do not load the entire alibaba trace, too much.

    if "google" in str(folder).lower(): continue
    if "lanl" in str(folder).lower(): continue
    if "two_sigma" in str(folder).lower(): continue

    for tu, tsp, tpp, dvfs in itertools.product(target_utilizations, task_selection_policies,
                                                task_placement_policies, dvfs_enabled):

        experiment_name = f"{folder}_tu_{tu}_tsp_{tsp}_tpp_{tpp}_dvfs_{dvfs}"
        output_dir = os.path.join(output_location, experiment_name)
        if os.path.exists(output_dir):
            continue
        job_file = os.path.join(job_directory, f"{experiment_name}.job")

        with open(job_file, "w") as fh:
            command = "java -Xmx60g -cp /home/lvs215/wta-sim/target/wta-sim-0.1.jar science.atlarge.wta.simulator.WTASim -f wta"
            command += " -c " + " ".join([str(x) for x in machine_resources])
            command += " -t " + " ".join([str(x) for x in machine_tdps])
            command += " -bc " + " ".join([str(x) for x in machine_base_clocks])
            command += " -mf " + " ".join([str(x) for x in machine_fractions])
            command += " -sd " + slack_location
            command += " -e " + " ".join([str(x) for x in [dvfs] * len(machine_resources)])
            command += " -i " + os.path.join(trace_dir, folder)
            command += " -o " + output_dir
            command += " --target-utilization " + str(tu)
            command += " --task-order-policy " + tsp
            command += " --task-placement-policy " + tpp

            fh.writelines("#!/bin/bash\n")
            fh.writelines(f"#SBATCH --job-name={experiment_name}.job\n")
            fh.writelines(f"#SBATCH --output={experiment_name}.out\n")
            fh.writelines(f"#SBATCH --error={experiment_name}.err\n")
            fh.writelines("#SBATCH --time=48-00:00\n")
            fh.writelines(f"{command}\n")

        os.system(f"sbatch {job_file}")
