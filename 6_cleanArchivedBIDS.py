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
        
    for subject in subjects_state:
        last_job = max(subjects_state[subject])
        state = subjects_state[subject][last_job]
        bids_subject_folder = os.path.join(scratch_path,"ukbb","ukbb_bids",subject)
        if (state == "ARCHIVED") and (subject in archived_subjects) and os.path.exists(bids_subject_folder):
            #print(bids_subject_folder)
            shutil.rmtree(bids_subject_folder)

    print("Residual subject bids folder cleaned!")