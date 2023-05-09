import os
import json
import shutil
from dotenv import load_dotenv
import time
import subprocess

if __name__ == "__main__":
    load_dotenv()
    scratch_path=os.getenv('SCRATCH_PATH')
    
    subjects_state_path = os.path.join(scratch_path,"ukbb","scripts","data","subjects_state.json")
    archived_subjects_path = os.path.join(scratch_path,"ukbb","scripts","data","archived_subjects.json")
    script_path = os.path.join(scratch_path,"ukbb","scripts")
    
    time.sleep(2)
    print(subprocess.check_output(f"cd {script_path}; python stats.py", shell=True, text=True))
    time.sleep(2)

    with open(subjects_state_path,"r") as json_file:
        subjects_state = json.load(json_file)
    with open(archived_subjects_path,"r") as json_file:
        archived_subjects = json.load(json_file)
    
    
    i=0
    for subject in subjects_state:
        delete = False
        last_job = max(subjects_state[subject])
        state = subjects_state[subject][last_job]
        if (state == "ARCHIVED") or (state == "COMPLETED"):
            bids_subject_folder = os.path.join(scratch_path,"ukbb","ukbb_bids",subject)
            result_subject_folder = os.path.join(scratch_path,"ukbb","fmriprep",f"{subject}_fmriprep.tar.gz")
            workdir_subject_folder = os.path.join(scratch_path,"ukbb","workdir",f"fmriprep_{subject}.workdir")
        if (state == "ARCHIVED") and (subject in archived_subjects) and os.path.exists(bids_subject_folder):
            #print(bids_subject_folder)
            shutil.rmtree(bids_subject_folder)
            delete = True
        if (state == "ARCHIVED") and (subject in archived_subjects) and os.path.exists(result_subject_folder):
            #print(result_subject_folder)
            os.remove(result_subject_folder)
            delete = True
        if ((state == "ARCHIVED") or (state == "COMPLETED")) and (subject in archived_subjects) and os.path.exists(workdir_subject_folder):
            print("Cleaned workdir ",workdir_subject_folder)
            shutil.rmtree(workdir_subject_folder)
            delete = True
        if delete:
            i+=1
            
        if i!= 0 and (i % 100) == 0:
            print("Processed:", i, len(subjects_state))

    print("Residual subject bids folder cleaned!")