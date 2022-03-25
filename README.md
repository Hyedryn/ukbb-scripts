#1. download ukbb zip files using ukbbfetch utility in `${SCRATCH}/ukbb_zip_files`

#2. install datalad_ukbb inside a python 3.8 venv (use ukbfetch_surrogate.sh)
`~/.virtualenvs/datalad_ukbb`

#3. clone this repo in your HPC HOME
```
git clone https://github.com/ccna-biomarkers/ukbb_scripts.git
```

#4. copy `ukbb` dir at `${SCRATCH}/datasets/ukbb` on compute canada

#5. install https://github.com/ccna-biomarkers/ccna_ts_extraction inside a python 3.8 venv 
`~/.virtualenvs/ts_extraction`

#6. extract the segmented difumo atlas in `${SCRATCH}` avalaible on beluga (`/nearline/ctb-pbellec/atlases/segmented_difumo_atlases_2022-02-03.tar.gz`)
```
tar -zxvf segmented_difumo_atlases_2022-02-03.tar.gz -C ${SCRATCH}
```

#7. you can now submit a preprocessing job for one participant with:

```
sbatch --account=rrg-jacquese --job-name=fmriprep_ukbb_${PARTICIPANT_ID}_%A.job --mail-user=<REDACTED> --output=/scratch/%u/.slurm/fmriprep_ukbb_${PARTICIPANT_ID}_%A.out --error=/scratch/%u/.slurm/fmriprep_ukbb_${PARTICIPANT_ID}_%A.err ${HOME}/ukbb_scripts/fmriprep-slurm_ukbb.bash ${PARTICIPANT_ID}
```
