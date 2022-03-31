#!/bin/bash
#SBATCH --time=1:00:00
#SBATCH --cpus-per-task=1
#SBATCH --mem=48G
#SBATCH --mail-type=BEGIN
#SBATCH --mail-type=END
#SBATCH --mail-type=FAIL

PARTICIPANT_ID=$1

DATASET_DIR=${SCRATCH}/datasets/ukbb
DATASET_TMPDIR=${SLURM_TMPDIR}/ukbb
tar_fmriprep=${PARTICIPANT_ID}_fmriprep.tar.gz
tar_timeseries=${PARTICIPANT_ID}_timeseries.tar.gz
export SINGULARITYENV_TEMPLATEFLOW_HOME=/templateflow

module load singularity/3.8

### Data preparation
# copying root dataset and preprocessed data into local scratch space
rsync -rlt --exclude "*.tar.gz" ${DATASET_DIR} ${SLURM_TMPDIR}
scp /lustre06/nearline/6035398/preprocessed_data/ukbb/fmriprep/$tar_fmriprep ${DATASET_TMPDIR}/derivatives/fmriprep/fmriprep/
tar -xzf ${DATASET_TMPDIR}/derivatives/fmriprep/fmriprep/$tar_fmriprep -C ${DATASET_TMPDIR}/derivatives/fmriprep/fmriprep/
rm -rf ${DATASET_TMPDIR}/derivatives/fmriprep/fmriprep/$tar_fmriprep
# syncing custom atlases (like difumo segmented)
scp -r $SCRATCH/atlases $SLURM_TMPDIR/

### ts extraction
source ~/.virtualenvs/ts_extraction/bin/activate
python3 ~/ccna_ts_extraction/extract_timeseries_tar.py -i ${DATASET_TMPDIR}/derivatives/fmriprep/fmriprep/ --atlas-path ${SLURM_TMPDIR}/atlases --dataset-name ukbb_sub-${PARTICIPANT_ID} -o ${SLURM_TMPDIR}
deactivate

### tar and syncing files
mkdir -p ${SLURM_TMPDIR}/ukbb.timeseries && scp -r ${SLURM_TMPDIR}/dataset*/* ${SLURM_TMPDIR}/ukbb.timeseries/
cd ${SLURM_TMPDIR}/ukbb.timeseries && tar -czf ${SLURM_TMPDIR}/$tar_timeseries sub-${PARTICIPANT_ID}
mkdir -p /lustre06/nearline/6035398/preprocessed_data/ukbb.timeseries && scp ${SLURM_TMPDIR}/$tar_timeseries /lustre06/nearline/6035398/preprocessed_data/ukbb.timeseries/

# clean compute node
chmod u+rwx -R ${SLURM_TMPDIR} && rm -rf ${SLURM_TMPDIR}/*

exit 0
