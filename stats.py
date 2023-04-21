import json
import os
import subprocess
from dotenv import load_dotenv

def get_all_job_state():
    """
    Retrieve the state of a job through the sacct bash command offered by the slurm Workload Manager.
    :param job_id: The id of the job to retrieve the state of.
    :return state: The string value representing the state of the job.
    """
    cmd = "sacct -n -o jobid%30,state --starttime=2023-03-01 -u qdessain"

    proc = subprocess.Popen(cmd, universal_newlines=True,
                            shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    
    states = out.split('\n')
    states_dic = {}
    for line in states:
        if ".batch" in line or ".extern" in line:
            continue
        
        tmp = " ".join(line.split())
        tmp = tmp.split(" ")
        if len(tmp) != 2:
            continue
        states_dic[int(tmp[0])] = tmp[1]
    return states_dic


def get_job_state(job_id):
    """
    Retrieve the state of a job through the sacct bash command offered by the slurm Workload Manager.
    :param job_id: The id of the job to retrieve the state of.
    :return state: The string value representing the state of the job.
    """
    cmd = "sacct --jobs=" + str(job_id) + " -n -o state"

    proc = subprocess.Popen(cmd, universal_newlines=True,
                            shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    try:
        state = (out.partition('\n')[0]).rstrip().strip()
    except Exception:
        try:
            state = out.rstrip().strip()
        except Exception:
            print("Double error" + out)
            state = "NOSTATE"
    return state
    
if __name__ == "__main__":
    scratch_path = os.getenv('SCRATCH_PATH')
    slurm_jobs_path = os.path.join(scratch_path,"ukbb","scripts","data","slurm_jobs.json")
    subjects_state_path = os.path.join(scratch_path,"ukbb","scripts","data","subjects_state.json")
    failed_state_path = os.path.join(scratch_path,"ukbb","scripts","data","subjects_error.json")
    if os.path.isfile(subjects_state_path):
        with open(subjects_state_path,"r") as json_file:
            subjects_state = json.load(json_file)
    else:
        subjects_state = {}
    
    status = {}
    status_subject = {}
    failed_detail = {}
    
    if os.path.isfile(slurm_jobs_path):
        with open(slurm_jobs_path,"r") as json_file:
            slurm_jobs = json.load(json_file)
    else:
        print("slurm_jobs_path not found. Exiting...")
        exit(-1)
    
    archived_subjects_path = os.path.join(scratch_path,"ukbb","scripts","data","archived_subjects.json")
    with open(archived_subjects_path,"r") as json_file:
        archived_subjects = json.load(json_file)
        
    print("Getting all job states...")
    states_dic = get_all_job_state()
         
    analysed_job = {}
    for subject in slurm_jobs.keys():
        new = slurm_jobs[subject] not in analysed_job
        if new:
            #state = get_job_state(slurm_jobs[subject])
            state = states_dic[slurm_jobs[subject]]
            analysed_job[slurm_jobs[subject]] = state
        else:
            state = analysed_job[slurm_jobs[subject]]
        
        state_job = state
        if state == "FAILED" or state == "TIMEOUT" or state == "NODE_FAIL":
            if os.path.exists(os.path.join(scratch_path,"ukbb","fmriprep",f"{subject}_fmriprep.tar.gz")) and os.path.exists(os.path.join(scratch_path,"ukbb","COMPLETED",subject)):
                print(f"Subject {subject} flaged as {state} but is COMPLETED!")
                state = "COMPLETED"
            elif os.path.exists(os.path.join(scratch_path,"ukbb","FAILED",subject)) and state != "FAILED":
                print(f"Subject {subject} flaged as {state} but is FAILED!")
                state = "FAILED"
        
        if subject not in subjects_state:
            subjects_state[subject] = {}
        subjects_state[subject][str(slurm_jobs[subject])] = state
        
        
        #Will be displayed as archived
        if state == "COMPLETED" and subject in archived_subjects:
            #subjects_state[subject]["9999999999"] = "ARCHIVED"
            state = "ARCHIVED"
        
        if state_job in status.keys():
            if new:
                status[state_job] += 1
        else:
            status[state_job] = 1
            
        if state in status_subject.keys():
            status_subject[state] += 1
        else:
            status_subject[state] = 1
        
    print("Job status: ",status)
    print("Subjects status: ",status_subject)
    
    with open(subjects_state_path,"w") as json_file:
        json.dump(subjects_state, json_file, indent=4)
        
        
    with open(subjects_state_path,"w") as json_file:
        json.dump(subjects_state, json_file, indent=4)
