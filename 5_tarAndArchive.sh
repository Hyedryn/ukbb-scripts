#!/bin/bash
#SBATCH --account=def-pbellec
#SBATCH --job-name=fmriprep_archive.job
#SBATCH --output=/lustre04/scratch/qdessain/ukbb/nearline/fmriprep_archive.out
#SBATCH --error=/lustre04/scratch/qdessain/ukbb/nearline/fmriprep_archive.err
#SBATCH --time=23:30:00
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=3896M
#SBATCH --mail-user=quentin.dessain@uclouvain.be
#SBATCH --mail-type=FAIL

cd /lustre04/scratch/qdessain/ukbb/scripts/
python 5_tarAndArchive.py
#while python tar_and_archive.py
#do
#  echo "Preparing next archive batch"
#done
#echo "End of archiving"
