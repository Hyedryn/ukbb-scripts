import json
import subprocess
import os
from multiprocessing.pool import Pool
import time
from dotenv import load_dotenv
import sys
import importlib
gen_slurm_batch = getattr(importlib.import_module("3_CreateSlurmBatches"), "gen_slurm_batch")
       
if __name__ == "__main__":
    load_dotenv()
    scratch_path=os.getenv('SCRATCH_PATH')
    batch_size=int(os.getenv('BATCH_SIZE'))
    max_jobs_count=int(os.getenv('MAX_JOBS_COUNT'))
    email=os.getenv('SLURM_EMAIL')
    username=os.getenv('USERNAME')
    
    with open(os.path.join(scratch_path,"ukbb","scripts","data","json_stats.json"), "r") as json_file:
        json_stats = json.load(json_file)
        print("[fMRI stats] noJSON: ",len(json_stats["noJSON"]), "wrongSliceTiming: ",len(json_stats["wrongSliceTiming"]), "validJSON: ",len(json_stats["validJSON"]))
        print("[fMRI stats] total entries: ",len(json_stats["noJSON"])+len(json_stats["wrongSliceTiming"])+len(json_stats["validJSON"]))
    ukbb_subjects = json_stats["validJSON"] + json_stats["noJSON"] + json_stats["wrongSliceTiming"]
    
    output_path = os.path.join(scratch_path,"ukbb","ukbb_bids")
    
    archived_subjects_path = os.path.join(scratch_path,"ukbb","scripts","data","archived_subjects.json")
    with open(archived_subjects_path,"r") as json_file:
        archived_subjects = json.load(json_file)
    
    effective_batch = []
    for sub in os.listdir(output_path):
        if sub in ukbb_subjects and sub not in archived_subjects :
            effective_batch.append(sub)
    effective_batch_size = len(effective_batch)
    
    #####################################################
    #                 Run slurm batches                 #
    #####################################################
    
    print(f"Launching {effective_batch_size} slurm batches...")
    script_path = os.path.join(scratch_path,"ukbb","scripts")
    
    slurm_jobs_path = os.path.join(script_path,"data","slurm_jobs.json")
    if os.path.isfile(slurm_jobs_path):
        with open(slurm_jobs_path,"r") as json_file:
            slurm_jobs = json.load(json_file)
    else:
        slurm_jobs = {}
    
    job_history_path = os.path.join(script_path,"data","job_history.json")
    if os.path.isfile(job_history_path):
        with open(job_history_path,"r") as json_file:
            job_history = json.load(json_file)
    else:
        job_history = {}
    
    print(subprocess.check_output(f"cd {script_path}; python stats.py", shell=True, text=True))
    subjects_state_path = os.path.join(script_path,"data","subjects_state.json")
    with open(subjects_state_path,"r") as json_file:
        subjects_state = json.load(json_file)
    
    active_jobs_count = int(subprocess.check_output(f"squeue -u {username} | wc -l", shell=True, text=True))-1
    print(f"There are {active_jobs_count} active jobs")
    
    jobs_count = active_jobs_count
    i = 0
    for subject in effective_batch:
        if jobs_count>=max_jobs_count:
            break
        
        if subject in subjects_state:
            last_job = max(subjects_state[subject])
            state = subjects_state[subject][last_job]
            if state in ["NODE_FAIL"]:
                pass
            elif state in ["TIMEOUT"]:
                print(f"Subject {subject} timeout, relaunching needed with longer timeout!")
                print(subprocess.check_output(f"sacct --jobs={last_job} -n -o jobid%20,state,Elapsed,Timelimit --starttime=2023-03-01 -u {username}", shell=True, text=True).partition("\n")[0])
                gen_slurm_batch(subject, scratch_path, email, timeout="48:00:00")
                #continue
            elif state in ["FAILED"]:
                print(f"Subject {subject} failed, investigation needed!")
                print(subprocess.check_output(f"sacct --jobs={last_job} -n -o jobid%20,state,Elapsed,Timelimit --starttime=2023-03-01 -u {username}", shell=True, text=True).partition("\n")[0])
                continue
            elif state in ["PENDING", "RUNNING", "COMPLETED", "ARCHIVED"]:
                continue
            else:
                print(f"unknow state {state} for subject {subject}")
                continue
        
        slurm_cmd = f"sbatch {scratch_path}/ukbb/.slurm/fmriprep_{subject}.sh"
        try:
            sbatch_output = subprocess.check_output(slurm_cmd, shell=True, text=True)
        except subprocess.CalledProcessError as e:
            if sbatch_output is not None:
                print("ERROR: Subprocess call output: %s" % sbatch_output)
            raise e
        print(sbatch_output)
        
        if "Submitted batch job" in sbatch_output:
            slurm_jobs[subject] = int(sbatch_output.split(" ")[-1])
            jobs_count += 1
            job_history[str(int(sbatch_output.split(" ")[-1]))] = subject
        else:
            print(f"Failed to launch subject {subject}")
            slurm_jobs[subject] = -1
        
        if i % 500 == 0:
            print("",i,"/",len(effective_batch), "slurm batches launched (",100*(i/len(effective_batch)) ,"%)")
        i += 1
    
    active_jobs_count = int(subprocess.check_output(f"squeue -u {username} | wc -l", shell=True, text=True))-1
    print(f"There are now {active_jobs_count} active jobs")
        
    with open(slurm_jobs_path,"w") as json_file:
            json.dump(slurm_jobs, json_file, indent=4)
            
    with open(job_history_path,"w") as json_file:
            json.dump(job_history, json_file, indent=4)
        
    print("All slurm batches launched!")
    
    time.sleep(3)
    print(subprocess.check_output(f"cd {script_path}; python stats.py", shell=True, text=True))
        
