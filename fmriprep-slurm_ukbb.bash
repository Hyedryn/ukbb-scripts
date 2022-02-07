#!/bin/bash

#SBATCH --time=24:00:00
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=8000M
#SBATCH --mail-type=BEGIN
#SBATCH --mail-type=END
#SBATCH --mail-type=FAIL

PARTICIPANT_ID=$1

DATASET_DIR=${SCRATCH}/datasets/ukbb
OUTPUT_DIR=${DATASET_DIR}
export SINGULARITYENV_FS_LICENSE=${HOME}/.freesurfer.txt
export SINGULARITYENV_TEMPLATEFLOW_HOME=/templateflow

module load singularity/3.8

# prepare input directory
#copying root dataset into local scratch space
rsync -rltv --info=progress2 --exclude "sub-*" ${DATASET_DIR} ${SLURM_TMPDIR}
DATASET_TMPDIR=${SLURM_TMPDIR}/ukbb
#bids-compatible subject data with ukb_datalad
source ~/.virtualenvs/datalad_ukbb/bin/activate
datalad create ${DATASET_TMPDIR}/sub-${PARTICIPANT_ID}
cd ${DATASET_TMPDIR}/sub-${PARTICIPANT_ID}
datalad ukb-init --bids --force ${PARTICIPANT_ID} 20227_2_0 20252_2_0
datalad ukb-update --merge --force --keyfile "./" --drop archives
rm -r sub-${PARTICIPANT_ID}/*/non-bids
cd ${SLURM_TMPDIR}
deactivate

# fmriprep job
singularity run --cleanenv -B ${SLURM_TMPDIR}:/WORK -B ${DATASET_TMPDIR}:/DATA -B ${HOME}/.cache/templateflow:/templateflow -B /etc/pki:/etc/pki/ \
    /lustre06/project/6002071/containers/fmriprep-20.2.1lts.sif \
    -w /WORK/fmriprep_work \
    --output-spaces MNI152NLin2009cAsym MNI152NLin6Asym \
    --notrack --write-graph --resource-monitor \
    --omp-nthreads 1 --nprocs 1 --mem_mb 8000 \
    --participant-label sub-${PARTICIPANT_ID} --random-seed 0 --skull-strip-fixed-seed \
    /DATA /DATA/derivatives/fmriprep participant
fmriprep_exitcode=$?

# save outputs
rsync -rltv --info=progress ${SLURM_TMPDIR}/fmriprep_work ${OUTPUT_DIR}/fmriprep_sub-${PARTICIPANT_ID}.workdir
if [ $fmriprep_exitcode -eq 0 ] ; then
    rsync -rltv --info=progress ${DATASET_TMPDIR}/derivatives ${OUTPUT_DIR}
    rsync -rltv --info=progress ${DATASET_TMPDIR}/sub-${PARTICIPANT_ID} ${OUTPUT_DIR}
    rm -r ${DATASET_TMPDIR}/derivatives/fmriprep/*
fi
rm -r ${DATASET_TMPDIR}/fmriprep_work

exit $fmriprep_exitcode 
