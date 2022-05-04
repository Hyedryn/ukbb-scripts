#!/bin/bash
#SBATCH --time=24:00:00
#SBATCH --cpus-per-task=1
#SBATCH --mem=8G
#SBATCH --mail-type=BEGIN
#SBATCH --mail-type=END
#SBATCH --mail-type=FAIL

PARTICIPANT_ID=$1

DATASET_DIR=${SCRATCH}/datasets/ukbb
DATASET_TMPDIR=${SLURM_TMPDIR}/ukbb
tar_raw=${PARTICIPANT_ID}_raw.tar.gz
tar_fmriprep=${PARTICIPANT_ID}_fmriprep.tar.gz
tar_qc_fmriprep=${PARTICIPANT_ID}_qc_fmriprep.tar
tar_freesurfer=${PARTICIPANT_ID}_freesurfer.tar.gz
tar_workdir=${PARTICIPANT_ID}_workdir.tar.gz
export SINGULARITYENV_FS_LICENSE=${HOME}/.freesurfer.txt
export SINGULARITYENV_TEMPLATEFLOW_HOME=/templateflow

module load singularity/3.8

# copying root dataset into local scratch space
rsync -rlt --exclude "*.tar.gz" ${DATASET_DIR} ${SLURM_TMPDIR}

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

### tar outputs
echo "tarring outputs..."
# raw
cd ${DATASET_TMPDIR} && tar -czf ${SLURM_TMPDIR}/$tar_raw sub-${PARTICIPANT_ID}
# derivatives and qc
cd ${DATASET_TMPDIR}/derivatives/fmriprep/fmriprep
tar -czf ${SLURM_TMPDIR}/$tar_fmriprep sub-${PARTICIPANT_ID}
tar -cf ${SLURM_TMPDIR}/$tar_qc_fmriprep sub-${PARTICIPANT_ID}.html
tar -uf ${SLURM_TMPDIR}/$tar_qc_fmriprep sub-${PARTICIPANT_ID}/figures
gzip -f ${SLURM_TMPDIR}/$tar_qc_fmriprep
# freesurfer
cd ${DATASET_TMPDIR}/derivatives/fmriprep/freesurfer && tar -czf ${SLURM_TMPDIR}/$tar_freesurfer sub-${PARTICIPANT_ID}
# fmriprep workdir
cd ${SLURM_TMPDIR} && tar -czf ${SLURM_TMPDIR}/$tar_workdir fmriprep_work

### tranfer archives
echo "transfer into user scratch and tape server"
# into scratch
scp ${SLURM_TMPDIR}/$tar_raw ${DATASET_DIR}/
scp ${SLURM_TMPDIR}/$tar_fmriprep ${DATASET_DIR}/derivatives/fmriprep/fmriprep/
scp ${SLURM_TMPDIR}/$tar_freesurfer ${DATASET_DIR}/derivatives/fmriprep/freesurfer/
mkdir -p ${DATASET_DIR}.qc && scp ${SLURM_TMPDIR}/$tar_qc_fmriprep.gz ${DATASET_DIR}.qc/
mkdir -p ${DATASET_DIR}.workdir && scp ${SLURM_TMPDIR}/$tar_workdir ${DATASET_DIR}.workdir/
# into tape
mkdir -p /lustre06/nearline/6035398/datasets/ukbb && scp ${SLURM_TMPDIR}/$tar_raw /lustre06/nearline/6035398/datasets/ukbb/
mkdir -p /lustre06/nearline/6035398/preprocessed_data/ukbb/fmriprep && scp ${SLURM_TMPDIR}/$tar_fmriprep /lustre06/nearline/6035398/preprocessed_data/ukbb/fmriprep
mkdir -p /lustre06/nearline/6035398/preprocessed_data/ukbb/freesurfer && scp ${SLURM_TMPDIR}/$tar_freesurfer /lustre06/nearline/6035398/preprocessed_data/ukbb/freesurfer
mkdir -p /lustre06/nearline/6035398/preprocessed_data/ukbb.qc && scp ${SLURM_TMPDIR}/$tar_qc_fmriprep.gz /lustre06/nearline/6035398/preprocessed_data/ukbb.qc/
mkdir -p /lustre06/nearline/6035398/preprocessed_data/ukbb.workdir && scp ${SLURM_TMPDIR}/$tar_workdir /lustre06/nearline/6035398/preprocessed_data/ukbb.workdir/
chmod a+rwx -R /lustre06/nearline/6035398/preprocessed_data/ukbb*
chmod a+rwx -R /lustre06/nearline/6035398/datasets/ukbb*

### clean compute node
chmod u+rwx -R ${SLURM_TMPDIR} && rm -rf ${SLURM_TMPDIR}/*

exit $fmriprep_exitcode 
