import os
import json
import subprocess
import uuid
import shutil
from dotenv import load_dotenv
import time

if __name__ == "__main__":
    load_dotenv()
    scratch_path = os.getenv('SCRATCH_PATH')
    slurm_tmp_dir = os.getenv('SLURM_TMPDIR')
    nearline_path = os.getenv('UKBB_NEARLINE_ARCHIVE_FOLDER')
    
    tarUUID = str(uuid.uuid4())
    tar_file = f"fmriprep_batch_{tarUUID}.tar"
    tmp_archive_dir = os.path.join(scratch_path,"ukbb","tmp_archive",tarUUID)
    os.makedirs(tmp_archive_dir, mode = 0o777, exist_ok = True)
    if not os.path.exists(tmp_archive_dir):
        exit(-1)
    
    print(f"Starting archiving with archive id {tarUUID}")
    
    ukbb_nearline_path = os.path.join(scratch_path,"ukbb","nearline")
    tar_path = os.path.join(scratch_path,"ukbb","nearline",tar_file)
    md5_path = os.path.join(scratch_path,"ukbb","nearline",f"{tarUUID}.md5")
    subjects_state_path = os.path.join(scratch_path,"ukbb","scripts","data","subjects_state.json")
    archived_subjects_path = os.path.join(scratch_path,"ukbb","scripts","data","archived_subjects.json")
    fmriprep_path = os.path.join(scratch_path,"ukbb","fmriprep")

    with open(subjects_state_path,"r") as json_file:
        subjects_state = json.load(json_file)
    with open(archived_subjects_path,"r") as json_file:
        archived_subjects = json.load(json_file)
    with open(archived_subjects_path,"r") as json_file:
        archived_subjects_dummy = json.load(json_file)
        
      
    #Dry run
    archive_filecount_dummy=0
    for subject in subjects_state:
        if archive_filecount_dummy >= 1000:
            break
        last_job = max(subjects_state[subject])
        state = subjects_state[subject][last_job]
        
        tar_archive = os.path.join(scratch_path,"ukbb","fmriprep",f"{subject}_fmriprep.tar.gz")
        ack_file = os.path.join(scratch_path,"ukbb","COMPLETED",subject)
        if (state == "COMPLETED") and os.path.exists(tar_archive) and os.path.exists(ack_file) and (subject not in archived_subjects_dummy):
            archive_filecount_dummy+=1
            archived_subjects_dummy.append(subject)
    
    
    print(f"[Dummy] Number of subjects to move to tmp archive dir: {archive_filecount_dummy}/1000.")
    
    if archive_filecount_dummy < 1000:
        print("Not enough subject to archive!")
        exit(-1)
            
    
    archive_filecount=0
    newly_archived_subjects = []
    for subject in subjects_state:
        if archive_filecount >= 1000:
            break
        last_job = max(subjects_state[subject])
        state = subjects_state[subject][last_job]
        
        tar_archive = os.path.join(scratch_path,"ukbb","fmriprep",f"{subject}_fmriprep.tar.gz")
        ack_file = os.path.join(scratch_path,"ukbb","COMPLETED",subject)
        if (state == "COMPLETED") and os.path.exists(tar_archive) and os.path.exists(ack_file) and (subject not in archived_subjects):
            shutil.move(tar_archive, tmp_archive_dir)
            archive_filecount+=1
            archived_subjects.append(subject)
            newly_archived_subjects.append(subject)
            print(f"Number of subjects moved to tmp archive dir: {archive_filecount}/1000.")
        elif (state == "COMPLETED") and os.path.exists(tar_archive) and os.path.exists(ack_file) and subject in archived_subjects:
            print(f"Warning {subject} is already archived!")
            
        #if (state != "COMPLETED"):
        #    print(f"Ignoring {subject} since its state is {state}!")
            
    # Generate the archive
    p = subprocess.run(f"cd {tmp_archive_dir}; tar -cvpf {tar_path} * | xargs -I '{{}}' sh -c \"test -f '{{}}' && md5sum '{{}}'\" | tee {md5_path} ", shell=True, text=True)
    if p.returncode != 0:
        print("Exception occured when generating the archive! ", p.returncode)
        exit(p.returncode)    
    
    # Add md5 to the archive
    p = subprocess.run(f"cd {ukbb_nearline_path}; tar -uf {tar_path} {tarUUID}.md5 ", shell=True, text=True)
    if p.returncode != 0:
        print("Exception occured when appending the md5 file to the archive! ", p.returncode)
        exit(p.returncode)    
    
    archive_filecount = subprocess.check_output(f"tar -tf {tar_path} | wc -l", shell=True, text=True)
    print("Final archive file count: ",archive_filecount)
    
    with open(archived_subjects_path,"w") as json_file:
        json.dump(archived_subjects, json_file, indent=4)
    
    # Rsync to nearline
    p = subprocess.run(f"rsync -rlt {tar_path} {nearline_path}", shell=True, text=True)
    if p.returncode != 0:
        print("Exception occured when syncing the archive to nearline! ", p.returncode)
        exit(p.returncode)
    
    # Clean tmp dir
    shutil.rmtree(tmp_archive_dir) 
    
    # Clean fmriprep dir
    for subject in newly_archived_subjects:
        last_job = max(subjects_state[subject])
        state = subjects_state[subject][last_job]
        tar_archive = os.path.join(scratch_path,"ukbb","fmriprep",f"{subject}_fmriprep.tar.gz")
        ack_file = os.path.join(scratch_path,"ukbb","COMPLETED",subject)
        if (state == "COMPLETED") and os.path.exists(tar_archive) and os.path.exists(ack_file):
            os.remove(tar_archive) 
        else:
            print(f"Error, tar_archive for subject {subject} do not exist")
    
    # Update stats
    script_path = os.path.join(scratch_path,"ukbb","scripts")
    time.sleep(2)
    print(subprocess.check_output(f"cd {script_path}; python stats.py", shell=True, text=True))
    time.sleep(2)
