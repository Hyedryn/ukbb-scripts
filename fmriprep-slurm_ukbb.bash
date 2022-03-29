#!/bin/bash

#SBATCH --time=26:00:00
#SBATCH --cpus-per-task=1
#SBATCH --mem=48G
#SBATCH --mail-type=BEGIN
#SBATCH --mail-type=END
#SBATCH --mail-type=FAIL

PARTICIPANT_ID=$1

DATASET_DIR=${SCRATCH}/datasets/ukbb
OUTPUT_DIR=${DATASET_DIR}
tar_preprocessed=${PARTICIPANT_ID}_raw_preprocessed.tar.gz
tar_workdir=${PARTICIPANT_ID}_workdir.tar.gz
export SINGULARITYENV_FS_LICENSE=${HOME}/.freesurfer.txt
export SINGULARITYENV_TEMPLATEFLOW_HOME=/templateflow

module load singularity/3.8

# copying root dataset into local scratch space
rsync -rlt --exclude "sub-*" --exclude "*.tar.gz" --exclude "*.workdir" --exclude "derivatives" ${DATASET_DIR} ${SLURM_TMPDIR}
DATASET_TMPDIR=${SLURM_TMPDIR}/ukbb

### UKBB bids-ification
#bids-compatible subject data with ukb_datalad
source ~/.virtualenvs/datalad-ukbb/bin/activate
datalad create ${DATASET_TMPDIR}/sub-${PARTICIPANT_ID}
cd ${DATASET_TMPDIR}/sub-${PARTICIPANT_ID}
datalad ukb-init --bids --force ${PARTICIPANT_ID} 20227_2_0 20252_2_0
datalad ukb-update --merge --force --keyfile "./" --drop archives
rm -r */non-bids
# fix task names if needed
for f in $(find . -name "*.json" -type l);
do 
    if grep $f -e "TaskName"; then
    	continue
    fi
    PATTERN=".*_task-(.*)_.*"
    [[ $f =~ $PATTERN ]]
    JSON_LINE='        "TaskName": "'${BASH_REMATCH[1]}'",'
    sed -i "2 i\\$JSON_LINE" $f
done
cd ${SLURM_TMPDIR}
deactivate

### Preprocessing
singularity run --cleanenv -B ${SLURM_TMPDIR}:/WORK -B ${DATASET_TMPDIR}:/DATA -B ${HOME}/.cache/templateflow:/templateflow -B /etc/pki:/etc/pki/ \
    /lustre06/project/6002071/containers/fmriprep-20.2.0lts.sif \
    -w /WORK/fmriprep_work \
    --output-spaces MNI152NLin2009cAsym MNI152NLin6Asym \
    --notrack --write-graph --resource-monitor \
    --omp-nthreads 1 --nprocs 1 --mem_mb 8000 \
    --participant-label sub-${PARTICIPANT_ID} --random-seed 0 --skull-strip-fixed-seed \
    /DATA /DATA/derivatives/fmriprep participant
fmriprep_exitcode=$?

# tar preprocessed outputs
cd ${DATASET_TMPDIR}
tar -czf ${SLURM_TMPDIR}/$tar_preprocessed .
cd ${SLURM_TMPDIR}/fmriprep_work
tar -czf ${SLURM_TMPDIR}/$tar_workdir .

### ts extraction
rsync -rlt $SCRATCH/atlases $SLURM_TMPDIR/
source ~/.virtualenvs/ts_extraction/bin/activate
python3 ~/ccna_ts_extraction/extract_timeseries_tar.py -i $SLURM_TMPDIR/ukbb/derivatives/fmriprep/fmriprep/ --atlas-path $SLURM_TMPDIR/atlases --dataset-name ukbb_sub-${PARTICIPANT_ID} -o $SLURM_TMPDIR
deactivate

### tranfer archives
scp ${SLURM_TMPDIR}/*.tar.gz  ${OUTPUT_DIR}/
chmod u+rwx -R ${SLURM_TMPDIR}
rm -rf ${SLURM_TMPDIR}/*

exit $fmriprep_exitcode 
