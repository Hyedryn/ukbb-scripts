import pandas as pd
import numpy as np
import os
import sys
import csv
import json
import subprocess
import shutil
from dotenv import load_dotenv

def gen_folder_structure(scratch_path):
    os.makedirs(os.path.join(scratch_path,"ukbb",".slurm"), exist_ok = True)
    os.makedirs(os.path.join(scratch_path,"ukbb",".slurm_multisubjects"), exist_ok = True)
    os.makedirs(os.path.join(scratch_path,"ukbb","COMPLETED"), exist_ok = True)
    os.makedirs(os.path.join(scratch_path,"ukbb","dataset_template"), exist_ok = True)
    os.makedirs(os.path.join(scratch_path,"ukbb","dataset_template","derivatives"), exist_ok = True)
    os.makedirs(os.path.join(scratch_path,"ukbb","FAILED"), exist_ok = True)
    os.makedirs(os.path.join(scratch_path,"ukbb","fmriprep"), exist_ok = True)
    os.makedirs(os.path.join(scratch_path,"ukbb","nearline"), exist_ok = True)
    os.makedirs(os.path.join(scratch_path,"ukbb","scripts"), exist_ok = True)
    os.makedirs(os.path.join(scratch_path,"ukbb","scripts","data"), exist_ok = True)
    os.makedirs(os.path.join(scratch_path,"ukbb","slurm_logs"), exist_ok = True)
    os.makedirs(os.path.join(scratch_path,"ukbb","slurm_logs_multisubjects"), exist_ok = True)
    os.makedirs(os.path.join(scratch_path,"ukbb","tmp_archive"), exist_ok = True)
    os.makedirs(os.path.join(scratch_path,"ukbb","ukbb_bids"), exist_ok = True)
    os.makedirs(os.path.join(scratch_path,"ukbb","workdir"), exist_ok = True)
    os.makedirs(os.path.join(scratch_path,"ukbb","ukbb_freesurfer"), exist_ok = True)
    
    script_path = os.path.join(scratch_path,"ukbb","scripts")
    
    slurm_jobs_path = os.path.join(script_path,"data","slurm_jobs.json")
    if not os.path.exists(slurm_jobs_path):
        with open(slurm_jobs_path,"x") as json_file:
            json.dump({}, json_file, indent=4)
            
    job_history_path = os.path.join(script_path,"data","job_history.json")
    if not os.path.exists(job_history_path):
        with open(job_history_path,"x") as json_file:
            json.dump({}, json_file, indent=4)
            
    subjects_state_path = os.path.join(script_path,"data","subjects_state.json")
    if not os.path.exists(subjects_state_path):
        with open(subjects_state_path,"x") as json_file:
            json.dump({}, json_file, indent=4)
            
    archived_subjects_path = os.path.join(script_path,"data","archived_subjects.json")
    if not os.path.exists(archived_subjects_path):
        with open(archived_subjects_path,"x") as json_file:
            json.dump([], json_file, indent=4)
            
    bids_filters_path = os.path.join(scratch_path,"ukbb","bids_filters.json")
    if not os.path.exists(bids_filters_path):
        bids_filters={"fmap": {"datatype": "fmap"}, "bold": {"datatype": "func", "suffix": "bold"}, "sbref": {"datatype": "func", "suffix": "sbref"}, "flair": {"datatype": "anat", "suffix": "FLAIR"}, "t2w": {"datatype": "anat", "suffix": "T2w"}, "t1w": {"datatype": "anat", "suffix": "T1w"}, "roi": {"datatype": "anat", "suffix": "roi"}}
        with open(bids_filters_path,"x") as json_file:
            json.dump(bids_filters, json_file, indent=4)
    
    dataset_desc_path = os.path.join(scratch_path,"ukbb","dataset_template","dataset_description.json")
    if not os.path.exists(dataset_desc_path):
        dataset_desc={"Name": "UK Biobank","BIDSVersion": "1.2.0","License": "TBD","Authors": ["A","B"],"Acknowledgements": "TBD","HowToAcknowledge":"TBD","Funding": ["TBD"],"ReferencesAndLinks": ["TBD"],"DatasetDOI": "TBD"}
        with open(dataset_desc_path,"x") as json_file:
            json.dump(dataset_desc, json_file, indent=4)

def get_json_stats(base_path,scratch_path):
    json_stats = {
        "noJSON": [],
        "wrongSliceTiming": [],
        "validJSON": [],
    }

    json_stats_T1 = {
        "noJSON": [],
        "validJSON": [],
    }

    t1_keys = []
    
    p = subprocess.run(f"cd {base_path}/ ; ls -1 > {scratch_path}/ukbb/scripts/data/BIDS_subjects.txt", shell=True, text=True)
    if p.returncode != 0:
        print("Unable to list UKBB subjects.")
        exit(-1)

    BIDS_df = pd.read_csv(os.path.join(scratch_path,"ukbb","scripts","data","BIDS_subjects.txt"),header=None,names=["BIDS"])
    tot = len(BIDS_df)
    i = 1
    for subject in BIDS_df["BIDS"]:
        json_path = os.path.join(base_path,subject,"func",subject+"_task-rest_bold.json")
        if os.path.exists(json_path):
            with open(json_path) as json_file:
                json_data = json.load(json_file)

                if np.max(json_data["SliceTiming"])  > json_data["RepetitionTime"]:
                    json_stats["wrongSliceTiming"].append(subject)
                else:
                    json_stats["validJSON"].append(subject)
        else:
            json_stats["noJSON"].append(subject)

        t1_path = os.path.join(base_path,subject,"anat",subject+"_T1w.json")
        if os.path.exists(t1_path):
            with open(t1_path) as t1_file:
                t1_data = json.load(t1_file)
                if len(t1_keys) == 0:
                    t1_keys = t1_data.keys()
                else:
                    if t1_keys != t1_data.keys():
                        print("Different keys for subject {}".format(subject))
                    else:
                        json_stats_T1["validJSON"].append(subject)
        else:
            json_stats_T1["noJSON"].append(subject)

        if i % 500 == 0:
            print(i,"/",tot, "subjects processed (",100*(i/tot) ,"%)")
        i += 1

    with open(os.path.join(scratch_path,"ukbb","scripts","data","json_stats_T1.json"), "w") as json_file:
        json.dump(json_stats_T1, json_file)

    with open(os.path.join(scratch_path,"ukbb","scripts","data","json_stats.json"), "w") as json_file:
        json.dump(json_stats, json_file)

if __name__ == "__main__":
    load_dotenv()
    base_path = os.getenv('UKBB_BIDS_FOLDER')
    scratch_path = os.getenv('SCRATCH_PATH')

    gen_folder_structure(scratch_path)
    
    if os.path.exists(base_path):
        if os.path.exists(os.path.join(scratch_path,"ukbb","scripts","data","json_stats.json")) or os.path.exists(os.path.join(scratch_path,"ukbb","scripts","data","json_stats_T1.json")):
            while True:
                answer = input("Json_stats file already exists. Continue?")
                if answer.lower() in ["y","yes"]:
                     break
                elif answer.lower() in ["n","no"]:
                     exit(0)
        get_json_stats(base_path, scratch_path)
    else:
        print("Unable to generate subjects json_stats, you may need to go to another HCP cluster to do that.")