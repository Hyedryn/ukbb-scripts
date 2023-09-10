import json
import subprocess
import os
from multiprocessing.pool import Pool
import time
from dotenv import load_dotenv
from pathlib import Path


def gen_QC_slurm_batch(md5_hash,scratch_path,email,ressource_account,timeout="23:59:00"):
    
    
    slurm_batch = f"""#!/bin/bash
#SBATCH --account={ressource_account}
#SBATCH --job-name=autoqc_{md5_hash}.job
#SBATCH --output={scratch_path}/ukbb/auto_qc/logs/autoqc_{md5_hash}.out
#SBATCH --error={scratch_path}/ukbb/auto_qc/logs/autoqc_{md5_hash}.err
#SBATCH --time={timeout}
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=4000M
#SBATCH --mail-user={email}
#SBATCH --mail-type=FAIL

export BASE_DIR={scratch_path}/ukbb/auto_qc/

md5_val="{md5_hash}"

mkdir -p $SLURM_TMPDIR/${{md5_val}}/archive/
mkdir -p $SLURM_TMPDIR/${{md5_val}}/workdir/
mkdir -p $SLURM_TMPDIR/${{md5_val}}/preproc/
mkdir -p $BASE_DIR/${{md5_val}}/output_qc/
mkdir -p $BASE_DIR/${{md5_val}}/output_connectome/

i=0
for FILE in `tar -tf ~/nearline/ctb-pbellec/giga_preprocessing_2/ukbb_fmriprep-20.2.7lts/fmriprep_batch_${{md5_val}}.tar `
do
	if [[ "$FILE" == "sub"*"_fmriprep.tar.gz" ]]; then
		subjID=${{FILE%%_*}}
		echo "Postprocessing subject $subjID ($i)"
		tar --directory $SLURM_TMPDIR/${{md5_val}}/archive/ -xvf ~/nearline/ctb-pbellec/giga_preprocessing_2/ukbb_fmriprep-20.2.7lts/fmriprep_batch_${{md5_val}}.tar $FILE 
		
		#echo "Copy done, extract individual archive"
		tar --exclude='fmriprep/layout_index.sqlite' --exclude='fmriprep/sourcedata/*' --exclude="fmriprep/${{subjID}}/log/*" --exclude="fmriprep/${{subjID}}/figures/*" --exclude='fmriprep/logs/*' -zxf $SLURM_TMPDIR/${{md5_val}}/archive/$FILE -C $SLURM_TMPDIR/${{md5_val}}/preproc/ fmriprep/${{subjID}}/func/${{subjID}}_task-rest_space-MNI152NLin2009cAsym_desc-brain_mask.nii.gz fmriprep/${{subjID}}/func/${{subjID}}_task-rest_desc-confounds_timeseries.tsv fmriprep/${{subjID}}/anat/${{subjID}}_space-MNI152NLin2009cAsym_desc-brain_mask.nii.gz
		
		echo "Running auto qc"
		giga_auto_qc $SLURM_TMPDIR/${{md5_val}}/preproc/fmriprep $BASE_DIR/${{md5_val}}/output_qc participant
		
		#echo "Running connectome"
		#giga_connectome $SLURM_TMPDIR/${{md5_val}}/preproc/fmriprep $BASE_DIR/${{md5_val}}/output_connectome participant

		#echo "Cleaning"
		rm -Rf $SLURM_TMPDIR/${{md5_val}}/archive/*
		rm -Rf $SLURM_TMPDIR/${{md5_val}}/workdir/*
		rm -Rf $SLURM_TMPDIR/${{md5_val}}/preproc/*
		((i=i+1))
	fi
done

"""
    
    with open(f"{scratch_path}/ukbb/auto_qc/.slurm/autoqc_{md5_hash}.sh", "w") as f:
        f.write(slurm_batch)

    return slurm_batch
    
if __name__ == "__main__":
    load_dotenv()
    scratch_path=os.getenv('SCRATCH_PATH')
    nearline_path = os.getenv('UKBB_NEARLINE_ARCHIVE_FOLDER')
    email=os.getenv('SLURM_EMAIL')
    ressource_account="def-jacquese" #os.getenv('RESSOURCE_ACCOUNT')
    
    genSlurmBatches = True
    launchSlurmBatches = True
    
    md5_list = []
    for dir in os.listdir(nearline_path):
        if "fmriprep_batch_" in dir and ".tar" in dir:
            md5_list.append(str(dir).split("_")[2].split(".")[0])
    
    if genSlurmBatches:
        i = 0
        for md5_hash in md5_list:
            gen_QC_slurm_batch(md5_hash, scratch_path, email, ressource_account, timeout="23:59:00")
            print(md5_hash, " ", i, "slurm batches generated")
            i += 1
    
    if launchSlurmBatches:
        for md5_hash in md5_list:
            if not os.path.exists(os.path.join(scratch_path,"ukbb","auto_qc",md5_hash)):
                slurm_cmd = f"sbatch {scratch_path}/ukbb/auto_qc/.slurm/autoqc_{md5_hash}.sh"
                try:
                    sbatch_output = subprocess.check_output(slurm_cmd, shell=True, text=True)
                except subprocess.CalledProcessError as e:
                    if sbatch_output is not None:
                        print("ERROR: Subprocess call output: %s" % sbatch_output)
                    raise e
                print(sbatch_output)
            else:
                print(f"Skipping archive {md5_hash} since the folder already exist!")
