import json
import subprocess
import os
from multiprocessing.pool import Pool
import time
from dotenv import load_dotenv

def gen_slurm_batch(subject1, subject2, subject3, subject4, subject5, subject6, jobName, scratch_path, email, timeout="38:00:00"):
    
    core = str(6)
    
    slurm_batch = f"""#!/bin/bash
#SBATCH --account=rrg-pbellec
#SBATCH --job-name=fmriprep_{jobName}.job
#SBATCH --output={scratch_path}/ukbb/slurm_logs_multisubjects/fmriprep_{jobName}.out
#SBATCH --error={scratch_path}/ukbb/slurm_logs_multisubjects/fmriprep_{jobName}.err
#SBATCH --time={timeout}
#SBATCH --cpus-per-task={core}
#SBATCH --mem-per-cpu=3968M
#SBATCH --mail-user={email}
#SBATCH --mail-type=FAIL


function run_subject {{
    export SINGULARITYENV_FS_LICENSE=$HOME/.freesurfer.txt
    export SINGULARITYENV_TEMPLATEFLOW_HOME=/templateflow
    
    module load singularity/3.8
    
    export subject=$1

    #clear previous job status
    rm {scratch_path}/ukbb/FAILED/${{subject}} -f
    rm {scratch_path}/ukbb/COMPLETED/${{subject}} -f
    rm {scratch_path}/ukbb/COMPLETED/${{subject}} -f

    #copying input dataset into local scratch space
    mkdir -p ${{SLURM_TMPDIR}}/${{subject}}/ukbb/${{subject}}/
    rsync -rlt --exclude "*.tar.gz" {scratch_path}/ukbb/dataset_template/ ${{SLURM_TMPDIR}}/${{subject}}/ukbb/
    rsync -rlt {scratch_path}/ukbb/ukbb_bids/${{subject}}/ ${{SLURM_TMPDIR}}/${{subject}}/ukbb/${{subject}}/
    rsync -rlt {scratch_path}/ukbb/bids_filters.json $SLURM_TMPDIR/${{subject}}/bids_filters.json

    cd ${{SLURM_TMPDIR}}

    singularity run --cleanenv -B ${{SLURM_TMPDIR}}/${{subject}}:/DATA -B ${{HOME}}/.cache/templateflow:/templateflow -B /etc/pki:/etc/pki/ /lustre03/project/6003287/containers/fmriprep-20.2.7lts.sif -w /DATA/fmriprep_work --participant-label ${{subject}} --output-spaces MNI152NLin2009cAsym MNI152NLin6Asym --output-layout bids --notrack --write-graph --omp-nthreads 1 --nprocs 1 --mem_mb 3712 --bids-filter-file /DATA/bids_filters.json --ignore slicetiming --random-seed 0 /DATA/ukbb /DATA/ukbb/derivatives/fmriprep participant 
    fmriprep_exitcode=$?

    if [ $fmriprep_exitcode -ne 0 ]
    then 
        rm {scratch_path}/ukbb/workdir/fmriprep_${{subject}}.workdir -rf
        mkdir -p {scratch_path}/ukbb/workdir/fmriprep_${{subject}}.workdir
        rsync -rlt $SLURM_TMPDIR/${{subject}}/fmriprep_work/ {scratch_path}/ukbb/workdir/fmriprep_${{subject}}.workdir
        rsync -rlt $SLURM_TMPDIR/${{subject}}/ukbb/derivatives/ {scratch_path}/ukbb/workdir/fmriprep_${{subject}}.workdir/
        touch {scratch_path}/ukbb/FAILED/${{subject}}
    fi 

    if [ $fmriprep_exitcode -eq 0 ] 
    then 
        mkdir -p $SLURM_TMPDIR/${{subject}}/tar/
        cd $SLURM_TMPDIR/${{subject}}/ukbb/derivatives/
        tar -cf $SLURM_TMPDIR/${{subject}}/tar/${{subject}}_fmriprep.tar fmriprep/
        cd {scratch_path}/ukbb/.slurm/
        tar -uf $SLURM_TMPDIR/${{subject}}/tar/${{subject}}_fmriprep.tar fmriprep_${{subject}}.sh
        cd {scratch_path}/ukbb/slurm_logs/
        tar -uf $SLURM_TMPDIR/${{subject}}/tar/${{subject}}_fmriprep.tar fmriprep_${{subject}}.out
        cd {scratch_path}/ukbb/slurm_logs/
        tar -uf $SLURM_TMPDIR/${{subject}}/tar/${{subject}}_fmriprep.tar fmriprep_${{subject}}.err
        cd $SLURM_TMPDIR/${{subject}}/tar/
        gzip -f $SLURM_TMPDIR/${{subject}}/tar/${{subject}}_fmriprep.tar
        rsync -rlt $SLURM_TMPDIR/${{subject}}/tar/${{subject}}_fmriprep.tar.gz {scratch_path}/ukbb/fmriprep/
        touch {scratch_path}/ukbb/COMPLETED/${{subject}}
    fi

    return $fmriprep_exitcode

}}

echo "Launching subjects {subject1}_{subject2}_{subject3}_{subject4}_{subject5}_{subject6}";
module load singularity/3.8

run_subject {subject1} 2>{scratch_path}/ukbb/slurm_logs/fmriprep_{subject1}.err 1>{scratch_path}/ukbb/slurm_logs/fmriprep_{subject1}.out &
run_subject {subject2} 2>{scratch_path}/ukbb/slurm_logs/fmriprep_{subject2}.err 1>{scratch_path}/ukbb/slurm_logs/fmriprep_{subject2}.out &
run_subject {subject3} 2>{scratch_path}/ukbb/slurm_logs/fmriprep_{subject3}.err 1>{scratch_path}/ukbb/slurm_logs/fmriprep_{subject3}.out &
run_subject {subject4} 2>{scratch_path}/ukbb/slurm_logs/fmriprep_{subject4}.err 1>{scratch_path}/ukbb/slurm_logs/fmriprep_{subject4}.out &
run_subject {subject5} 2>{scratch_path}/ukbb/slurm_logs/fmriprep_{subject5}.err 1>{scratch_path}/ukbb/slurm_logs/fmriprep_{subject5}.out &
run_subject {subject6} 2>{scratch_path}/ukbb/slurm_logs/fmriprep_{subject6}.err 1>{scratch_path}/ukbb/slurm_logs/fmriprep_{subject6}.out &

FAIL=0
for job in `jobs -p`
do
echo $job
    wait $job || let "FAIL+=1"
done

echo "End of multisubject slurm script with $FAIL failed job.";
echo "Subjects {subject1}_{subject2}_{subject3}_{subject4}_{subject5}_{subject6}";
exit $FAIL

"""
    
    with open(f"{scratch_path}/ukbb/.slurm_multisubjects/fmriprep_{jobName}.sh", "w") as f:
        f.write(slurm_batch)

    return slurm_batch
    
    
    
if __name__ == "__main__":
    load_dotenv()
    scratch_path=os.getenv('SCRATCH_PATH')
    batch_size=int(os.getenv('BATCH_SIZE'))
    max_jobs_count=int(os.getenv('MAX_JOBS_COUNT'))
    username=os.getenv('USERNAME')
    email=os.getenv('SLURM_EMAIL')
    
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
    queue = []
    i = 0
    for subject in effective_batch:
        if jobs_count>=max_jobs_count:
            break
            
        if len(queue) == 6:
            jobName = (queue[0].replace("sub-","") + queue[1] + queue[2] + queue[3] + queue[4]+ queue[5]).replace("sub-","_")
            gen_slurm_batch(queue[0],queue[1],queue[2],queue[3],queue[4],queue[5],jobName, scratch_path, email)
            slurm_cmd = f"sbatch {scratch_path}/ukbb/.slurm_multisubjects/fmriprep_{jobName}.sh"
            try:
                sbatch_output = subprocess.check_output(slurm_cmd, shell=True, text=True)
            except subprocess.CalledProcessError as e:
                if sbatch_output is not None:
                    print("ERROR: Subprocess call output: %s" % sbatch_output)
                raise e
            print(sbatch_output)
            
            if "Submitted batch job" in sbatch_output:
                slurm_jobs[queue[0]] = int(sbatch_output.split(" ")[-1])
                slurm_jobs[queue[1]] = int(sbatch_output.split(" ")[-1])
                slurm_jobs[queue[2]] = int(sbatch_output.split(" ")[-1])
                slurm_jobs[queue[3]] = int(sbatch_output.split(" ")[-1])
                slurm_jobs[queue[4]] = int(sbatch_output.split(" ")[-1])
                slurm_jobs[queue[5]] = int(sbatch_output.split(" ")[-1])
                jobs_count += 1
                job_history[str(int(sbatch_output.split(" ")[-1]))] = queue
            else:
                print(f"Failed to launch subject {subject}")
                slurm_jobs[queue[0]] = -1
                slurm_jobs[queue[1]] = -1
                slurm_jobs[queue[2]] = -1
                slurm_jobs[queue[3]] = -1
                slurm_jobs[queue[4]] = -1
                slurm_jobs[queue[5]] = -1
            
            queue = []
            
        if subject in subjects_state:
            last_job = max(subjects_state[subject])
            state = subjects_state[subject][last_job]
            if state in ["NODE_FAIL"]:
                pass
            elif state in ["TIMEOUT"]:
                print(f"Subject {subject} timeout, relaunching needed with longer timeout!")
                continue
            elif state in ["FAILED"]:
                print(f"Subject {subject} failed, investigation needed!")
                continue
            elif state in ["PENDING", "RUNNING", "COMPLETED", "ARCHIVED"]:
                continue
            else:
                print(f"unknow state {state} for subject {subject}")
                continue
                
        queue.append(subject)
            
        if i % 500 == 0:
            print("",i,"/",len(effective_batch), "slurm batches launched (",100*(i/len(effective_batch)) ,"%)")
        i += 1
        
    if len(queue) != 0:
        print("Residuals subject are left in the queue!", queue)
    
    active_jobs_count = int(subprocess.check_output(f"squeue -u {username} | wc -l", shell=True, text=True))-1
    print(f"There are now {active_jobs_count} active jobs")
        
    with open(slurm_jobs_path,"w") as json_file:
            json.dump(slurm_jobs, json_file, indent=4)
            
    with open(job_history_path,"w") as json_file:
            json.dump(job_history, json_file, indent=4)
        
    print("All slurm batches launched!")
    
    time.sleep(3)
    print(subprocess.check_output(f"cd {script_path}; python stats.py", shell=True, text=True))
        