import json
import subprocess
import os
from multiprocessing.pool import Pool
import time
from dotenv import load_dotenv
import zipfile

def gen_slurm_batch(subject,scratch_path,email,ressource_account,timeout="17:00:00",freesurfer=False):
    
    freesurfer_block = ""
    if freesurfer:
        freesurfer_block = f"""

if [ -f "$freesurfer_subject" ]; then
    echo "Using precomputed freesurfer output."
    mkdir -p ${{SLURM_TMPDIR}}/freesurfer/
    mkdir -p ${{SLURM_TMPDIR}}/freesurfer/{subject}/
    cd ${{SLURM_TMPDIR}}/freesurfer/{subject}/ ; unzip -qq $freesurfer_subject ; mv FreeSurfer/* . ; rm FreeSurfer/ -R
    #rsync -rlt {scratch_path}/ukbb/ukbb_freesurfer/{subject}/ ${{SLURM_TMPDIR}}/freesurfer/{subject}/
    freesurfer_parameter="--fs-subjects-dir /DATA/freesurfer/"
fi

"""
    
    slurm_batch = f"""#!/bin/bash
#SBATCH --account={ressource_account}
#SBATCH --job-name=fmriprep_{subject}.job
#SBATCH --output={scratch_path}/ukbb/slurm_logs/fmriprep_{subject}.out
#SBATCH --error={scratch_path}/ukbb/slurm_logs/fmriprep_{subject}.err
#SBATCH --time={timeout}
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=6496M
#SBATCH --mail-user={email}
#SBATCH --mail-type=FAIL

export SINGULARITYENV_FS_LICENSE=$HOME/.freesurfer.txt
export SINGULARITYENV_TEMPLATEFLOW_HOME=/templateflow

module load singularity/3.8

#clear previous job status
rm {scratch_path}/ukbb/FAILED/{subject} -f
rm {scratch_path}/ukbb/COMPLETED/{subject} -f

#copying input dataset into local scratch space
mkdir -p ${{SLURM_TMPDIR}}/ukbb/{subject}/
rsync -rlt --exclude "*.tar.gz" {scratch_path}/ukbb/dataset_template/ ${{SLURM_TMPDIR}}/ukbb/
rsync -rlt {scratch_path}/ukbb/ukbb_bids/{subject}/ ${{SLURM_TMPDIR}}/ukbb/{subject}/
rsync -rlt {scratch_path}/ukbb/bids_filters.json $SLURM_TMPDIR/bids_filters.json

freesurfer_subject={scratch_path}/ukbb/ukbb_freesurfer/{subject}_freesurfer.zip
freesurfer_parameter=""

{freesurfer_block}

cd ${{SLURM_TMPDIR}}

singularity run --cleanenv -B ${{SLURM_TMPDIR}}:/DATA -B ${{HOME}}/.cache/templateflow:/templateflow -B /etc/pki:/etc/pki/ /scratch/qdessain/fmriprep-20.2.7lts.sif -w /DATA/fmriprep_work --participant-label {subject} --output-spaces MNI152NLin2009cAsym MNI152NLin6Asym --output-layout bids --notrack --write-graph --omp-nthreads 1 --nprocs 1 --mem_mb 6150 --bids-filter-file /DATA/bids_filters.json --ignore slicetiming ${{freesurfer_parameter}} --stop-on-first-crash --random-seed 0 /DATA/ukbb /DATA/ukbb/derivatives/fmriprep participant 
fmriprep_exitcode=$?

if [ $fmriprep_exitcode -ne 0 ]
then 
    rm {scratch_path}/ukbb/workdir/fmriprep_{subject}.workdir -rf
    mkdir -p {scratch_path}/ukbb/workdir/fmriprep_{subject}.workdir
    rsync -rlt $SLURM_TMPDIR/fmriprep_work/ {scratch_path}/ukbb/workdir/fmriprep_{subject}.workdir
    rsync -rlt $SLURM_TMPDIR/ukbb/derivatives/ {scratch_path}/ukbb/workdir/fmriprep_{subject}.workdir/
    touch {scratch_path}/ukbb/FAILED/{subject}
fi 

if [ $fmriprep_exitcode -eq 0 ] 
then 
    mkdir -p $SLURM_TMPDIR/tar/
    cd $SLURM_TMPDIR/ukbb/derivatives/
    tar -cf $SLURM_TMPDIR/tar/{subject}_fmriprep.tar fmriprep/
    cd {scratch_path}/ukbb/.slurm/
    tar -uf $SLURM_TMPDIR/tar/{subject}_fmriprep.tar fmriprep_{subject}.sh
    cd {scratch_path}/ukbb/slurm_logs/
    tar -uf $SLURM_TMPDIR/tar/{subject}_fmriprep.tar fmriprep_{subject}.out
    cd {scratch_path}/ukbb/slurm_logs/
    tar -uf $SLURM_TMPDIR/tar/{subject}_fmriprep.tar fmriprep_{subject}.err
    cd $SLURM_TMPDIR/tar/
    gzip -f $SLURM_TMPDIR/tar/{subject}_fmriprep.tar
    rsync -rlt $SLURM_TMPDIR/tar/{subject}_fmriprep.tar.gz {scratch_path}/ukbb/fmriprep/
    touch {scratch_path}/ukbb/COMPLETED/{subject}
fi

exit $fmriprep_exitcode
"""
    
    with open(f"{scratch_path}/ukbb/.slurm/fmriprep_{subject}.sh", "w") as f:
        f.write(slurm_batch)

    return slurm_batch
       

if __name__ == "__main__":
    load_dotenv()
    scratch_path=os.getenv('SCRATCH_PATH')
    batch_size=int(os.getenv('BATCH_SIZE'))
    email=os.getenv('SLURM_EMAIL')
    ressource_account=os.getenv('RESSOURCE_ACCOUNT')
    
    genSlurmBatches=True
 
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
        if sub in ukbb_subjects and sub not in archived_subjects:
            effective_batch.append(sub)
    effective_batch_size = len(effective_batch)
    
    #####################################################
    #                 Gen slurm batches                 #
    #####################################################
    
    if genSlurmBatches:
        print(f"Generation of {effective_batch_size} slurm batches...")
        i = 0
        fs_count = 0
        for subject in effective_batch:
            freesurfer_sub_zip = os.path.join(scratch_path,"ukbb","ukbb_freesurfer",f"{subject}_freesurfer.zip")
            fs_orig = "FreeSurfer/mri/orig/001.mgz"
            if os.path.exists(freesurfer_sub_zip) and fs_orig in zipfile.ZipFile(freesurfer_sub_zip).namelist():
                gen_slurm_batch(subject, scratch_path, email, ressource_account,timeout="23:30:00",freesurfer=True)
                fs_count += 1
            else:
                gen_slurm_batch(subject, scratch_path, email, ressource_account,timeout="29:00:00",freesurfer=False)
            if i % 500 == 0:
                print("",i,"/",len(effective_batch), "slurm batches generated (",100*(i/len(effective_batch)) ,"%)")
                print("FS count:",fs_count)
            i += 1
            
