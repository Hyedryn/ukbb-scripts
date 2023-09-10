import json
import subprocess
import os
from multiprocessing.pool import Pool
import time
from dotenv import load_dotenv
from pathlib import Path
import random

def launch_Slurm_Batches(md5_list, atlas_cat, scratch_path, username):
    script_path = os.path.join(scratch_path,"ukbb","scripts")
    
    time.sleep(5)
    print(subprocess.check_output(f"cd {script_path}; python stats.py", shell=True, text=True))
    connectomes_state_path = os.path.join(script_path,"data","connectomes_state.json")
    with open(connectomes_state_path,"r") as json_file:
        connectomes_state = json.load(json_file)
        
    slurm_jobs_path = os.path.join(script_path,"data","connectomes_slurm_jobs.json")
    if os.path.isfile(slurm_jobs_path):
        with open(slurm_jobs_path,"r") as json_file:
            slurm_jobs = json.load(json_file)
    else:
        slurm_jobs = {}
    
    job_history_path = os.path.join(script_path,"data","connectomes_job_history.json")
    if os.path.isfile(job_history_path):
        with open(job_history_path,"r") as json_file:
            job_history = json.load(json_file)
    else:
        job_history = {}
    
    active_jobs_count = int(subprocess.check_output(f"squeue -u {username} | wc -l", shell=True, text=True))-1
    print(f"There are {active_jobs_count} active jobs")
    
    effective_batch = []
    for md5_hash in md5_list:
        for subpart in range(25):
            for atlas, mem, timeout in atlas_cat:
                effective_batch.append([atlas, md5_hash, str(subpart)])
    
    jobs_count = active_jobs_count
    i = 0
    for connectome_task in effective_batch:
        if jobs_count>=max_jobs_count:
            break
        
        task_name = '_'.join(connectome_task)
        atlasC = connectome_task[0]
        md5hashC = connectome_task[1]
        subpartC = connectome_task[2]
        if task_name in connectomes_state:
            last_job = max(connectomes_state[task_name])
            state = connectomes_state[task_name][last_job]
            if state in ["NODE_FAIL", "CANCELLED+"]:
                pass
            elif state in ["TIMEOUT"]:
                print(f"Connectome {task_name} timeout, relaunching needed with longer timeout!")
                pass
            elif state in ["FAILED"]:
                print(f"Connectome {task_name} failed, investigation needed!")
                continue
            elif state in ["PENDING", "RUNNING", "COMPLETED", "ARCHIVED"]:
                continue
            else:
                print(f"unknow state {state} for connectome {task_name}")
                continue
            
        slurm_cmd = f"sbatch {scratch_path}/ukbb/connectome/.slurm/connectome_{atlasC}_{md5hashC}_{subpartC}.sh"
        print(slurm_cmd)
        try:
            sbatch_output = subprocess.check_output(slurm_cmd, shell=True, text=True)
        except subprocess.CalledProcessError as e:
            if sbatch_output is not None:
                print("ERROR: Subprocess call output: %s" % sbatch_output)
            raise e
        print(sbatch_output)
        
        if "Submitted batch job" in sbatch_output:
            slurm_jobs[task_name] = int(sbatch_output.split(" ")[-1])
            jobs_count += 1
            job_history[str(int(sbatch_output.split(" ")[-1]))] = task_name
        else:
            print(f"Failed to launch connectome {task_name}")
            slurm_jobs[task_name] = -1
        
        if i % 100 == 0:
            print("",i,"/",len(effective_batch), "slurm batches launched (",100*(i/len(effective_batch)) ,"%)")
        i += 1
        
    with open(slurm_jobs_path,"w") as json_file:
        json.dump(slurm_jobs, json_file, indent=4)
            
    with open(job_history_path,"w") as json_file:
        json.dump(job_history, json_file, indent=4)
            
    active_jobs_count = int(subprocess.check_output(f"squeue -u {username} | wc -l", shell=True, text=True))-1
    print(f"There are now {active_jobs_count} active jobs")
    
    time.sleep(10)
    print(subprocess.check_output(f"cd {script_path}; python stats.py", shell=True, text=True))

def gen_Connectome_slurm_batch(md5_hash, atlas, subpart, scratch_path, email, ressource_account, timeout="23:59:00", memory = 12000):
    
    random_choice = random.random()
    if 0.75 < random_choice and random_choice <= 1.0:
        ressource_account = "def-pbellec"
    elif 0.5 < random_choice and random_choice <= 0.75:
        ressource_account = "def-jacquese"
        
    if atlas == "Schaefer20187Networks":
       ressource_account="def-pbellec"
    elif atlas == "MIST":
       ressource_account="def-jacquese"
    
    
    slurm_batch = f"""#!/bin/bash
#SBATCH --account={ressource_account}
#SBATCH --job-name=connectome_{atlas}_{subpart}_{md5_hash}.job
#SBATCH --output={scratch_path}/ukbb/connectome/logs/connectome_{atlas}_{subpart}_{md5_hash}.out
#SBATCH --error={scratch_path}/ukbb/connectome/logs/connectome_{atlas}_{subpart}_{md5_hash}.err
#SBATCH --time={timeout}
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu={memory}M
#SBATCH --mail-user={email}
#SBATCH --mail-type=FAIL

export BASE_DIR={scratch_path}/ukbb/connectome/

md5_val="{md5_hash}"
subpart={subpart}
start=$(( 40*subpart ))
end=$(( 40*subpart + 40 ))


mkdir -p $SLURM_TMPDIR/${{md5_val}}/${{subpart}}/archive/
mkdir -p $SLURM_TMPDIR/${{md5_val}}/${{subpart}}/workdir/
mkdir -p $SLURM_TMPDIR/${{md5_val}}/${{subpart}}/preproc/
mkdir -p $BASE_DIR/${{md5_val}}/${{subpart}}/output_connectome/

i=0
v=0
for FILE in `tar -tf ~/nearline/ctb-pbellec/giga_preprocessing_2/ukbb_fmriprep-20.2.7lts/fmriprep_batch_${{md5_val}}.tar `
do
    if [[ "$FILE" == "sub"*"_fmriprep.tar.gz" ]]; then
        subjID=${{FILE%%_*}}
        if [ $i -ge $start ]  && [ $i -lt $end ]; then
            ((v=v+1))
            echo "Postprocessing subject $subjID ($i) ($v)"
            
            COMPLETED_FILE={scratch_path}/ukbb/connectome/COMPLETED/${{subjID}}_{atlas}
            if test -f "$COMPLETED_FILE"; then
                echo "$COMPLETED_FILE exists. Skipping subject $subjID "
                continue
            fi
            
            tar --directory $SLURM_TMPDIR/${{md5_val}}/${{subpart}}/archive/ -xvf ~/nearline/ctb-pbellec/giga_preprocessing_2/ukbb_fmriprep-20.2.7lts/fmriprep_batch_${{md5_val}}.tar $FILE 
            
            tar --exclude='fmriprep/layout_index.sqlite' --exclude='fmriprep/sourcedata/*' --exclude="fmriprep/${{subjID}}/log/*" --exclude="fmriprep/${{subjID}}/figures/*" --exclude='fmriprep/logs/*' -zxf $SLURM_TMPDIR/${{md5_val}}/${{subpart}}/archive/$FILE -C $SLURM_TMPDIR/${{md5_val}}/${{subpart}}/preproc/ 
            
            echo "Running connectome"
            giga_connectome --denoise-strategy simple+gsr -w $SLURM_TMPDIR/${{md5_val}}/${{subpart}}/workdir/ --atlas {atlas} $SLURM_TMPDIR/${{md5_val}}/${{subpart}}/preproc/fmriprep $BASE_DIR/${{md5_val}}/${{subpart}}/output_connectome participant
            giga_connectome --denoise-strategy simple -w $SLURM_TMPDIR/${{md5_val}}/${{subpart}}/workdir/ --atlas {atlas} $SLURM_TMPDIR/${{md5_val}}/${{subpart}}/preproc/fmriprep $BASE_DIR/${{md5_val}}/${{subpart}}/output_connectome participant

            #echo "Cleaning"
            rm -Rf $SLURM_TMPDIR/${{md5_val}}/${{subpart}}/archive/*
            rm -Rf $SLURM_TMPDIR/${{md5_val}}/${{subpart}}/workdir/*
            rm -Rf $SLURM_TMPDIR/${{md5_val}}/${{subpart}}/preproc/*
            
            touch {scratch_path}/ukbb/connectome/COMPLETED/${{subjID}}_{atlas}
        fi
        ((i=i+1))
    fi
done

echo "Subjects processed: $v $i"

"""
    
    with open(f"{scratch_path}/ukbb/connectome/.slurm/connectome_{atlas}_{md5_hash}_{subpart}.sh", "w") as f:
        f.write(slurm_batch)

    return slurm_batch
    
if __name__ == "__main__":
    load_dotenv()
    scratch_path=os.getenv('SCRATCH_PATH')
    nearline_path = os.getenv('UKBB_NEARLINE_ARCHIVE_FOLDER')
    max_jobs_count=int(os.getenv('MAX_JOBS_COUNT'))
    email=os.getenv('SLURM_EMAIL')
    username=os.getenv('USERNAME')
    ressource_account="rrg-pbellec" #os.getenv('RESSOURCE_ACCOUNT')
    
    genSlurmBatches = True
    launchSlurmBatches = True
    
    md5_list = []
    for dir in os.listdir(nearline_path):
        if "fmriprep_batch_" in dir and ".tar" in dir:
            md5_list.append(str(dir).split("_")[2].split(".")[0])
    
    atlas_cat = [("Schaefer20187Networks",16000,"6:00:00"),("DiFuMo",36000,"23:00:00"),("MIST",16000,"16:00:00")]
    
    
    if genSlurmBatches:
        i = 0
        for md5_hash in md5_list:
            for subpart in range(25):
                for atlas, mem, timeout in atlas_cat:
                    gen_Connectome_slurm_batch(md5_hash, atlas, subpart, scratch_path, email, ressource_account, timeout=timeout, memory=mem)
                    print(md5_hash, " ", subpart, " ", atlas, " ", i, "slurm batches generated")
                    i += 1
    
    #atlas_cat = [("Schaefer20187Networks",16000,"6:00:00"),("MIST",16000,"16:00:00")]
    
    if launchSlurmBatches:
        launch_Slurm_Batches(md5_list, atlas_cat, scratch_path, username)
