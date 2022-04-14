# ukbb_scripts
This repository holds all scripts to preprocess ukbb using `ctb-pbellec` tape server and `rrg-jacquese` (or `def-jacquese`) allocation.

### First time setup

#### Environment

First you will need to connect to the server (for example beluga) and then go into your home.
```
ssh $USER@beluga.computecanada.ca
cd ~
```

1. Clone this repo in your HPC HOME
```
git clone https://github.com/ccna-biomarkers/ukbb_scripts.git
```

2. Create a virtual environment that will be used to initialize ukbb-bids with datalad
```
module load python/3.8
mkdir ~/.virtualenvs
python3 -m venv ~/.virtualenvs/datalad-ukbb
source ~/.virtualenvs/datalad-ukbb/bin/activate
python3 -m pip install -r $HOME/ukbb_scripts/requirements.txt
deactivate
```

3. To use ukbfetch with pre-downloaded data, install the surrogate file
```
ln -s $HOME/ukbb_scripts/ukbfetch_surrogate.sh $HOME/.virtualenvs/datalad-ukbb/bin/ukbfetch
```

4. Install the ukbb dataset layout
```
mkdir -p $SCRATCH/datasets
git clone https://github.com/ccna-biomarkers/ukbb-preprocess-template.git $SCRATCH/datasets/ukbb
```

5. Install the timeserie extraction tool in your home. Make sure that you setup ssh authentification with github by following [this tutorial](https://simexp-documentation.readthedocs.io/en/latest/tutorials/ssh.html).
```
cd ~
module load python/3.8
python3 -m venv ~/.virtualenvs/ts_extraction
source ~/.virtualenvs/ts_extraction/bin/activate
git clone https://github.com/ccna-biomarkers/ccna_ts_extraction.git
python3 -m pip install ccna_ts_extraction/requirements.txt
deactivate
```

####  Data

Download ukbb zip files using ukbbfetch utility and place the `zip` archives in `$SCRATCH/ukbb_zip_files`

>**Note**
>
>If you have access to the `rrg-jacquese` allocation on beluga, you can find the anatomical data at `~/ projects/rrg-jacquese/All_user_common_folder/RAW_DATA/UKBIOBANK-DATA/UKBIOBANK_IMAGING/UKB_MRI_download/UKB_T1w` and the functionnal data at `~/projects/rrg-jacquese/All_user_common_folder/RAW_DATA/UKBIOBANK-DATA/UKBIOBANK_IMAGING/UKB_MRI_download/UKB_rfMRI`.

Download all templates needed by fmriprep:
```
python3 -c "from templateflow.api import get; get(['MNI152NLin2009cAsym', 'MNI152NLin6Asym', 'OASIS30ANTs', 'MNIPediatricAsym', 'MNIInfant'])"
```

Get the segmented difumo atlas in your $SCRATCH avalaible on beluga.
```
mkdir -p ${SCRATCH}/atlases
scp beluga.computecanada.ca:/nearline/ctb-pbellec/atlases/segmented_difumo_atlases_2022-02-03.tar.gz $SCRATCH/atlases/
tar -zxvf $SCRATCH/atlases/segmented_difumo_atlases_2022-02-03.tar.gz -C ${SCRATCH}/atlases
scp -r ${SCRATCH}/atlases/segmented_difumo_atlases/* ${SCRATCH}/atlases && rm -r ${SCRATCH}/atlases/segmented_difumo_atlases && rm $SCRATCH/atlases/segmented_difumo_atlases_2022-02-03.tar.gz
```

### Usage

#### Launching preprocessing job

Submit a preprocessing job for one participant `PARTICIPANT_ID` with:

```
PARTICIPANT_ID=xxx
sbatch --account=def-xxx --job-name=fmriprep_ukbb_${PARTICIPANT_ID}_%j.job --mail-user="xxx@xxx.com" --output=/scratch/%u/.slurm/fmriprep_ukbb_${PARTICIPANT_ID}_%j.out --error=/scratch/%u/.slurm/fmriprep_ukbb_${PARTICIPANT_ID}_%j.err ${HOME}/ukbb_scripts/fmriprep-slurm_ukbb.bash ${PARTICIPANT_ID}
```

#### Sharing

After preprocessing, all the data will be availble at `$SCRATCH/datasets/ukbb`.
The preprocessing outputs is archived at `/nearline/ctb-pbellec/preprocessed_data/ukbb*` and raw data is at `/nearline/ctb-pbellec/datasets/ukbb`.

To download the data, you will manually copy into the desired system. For example if you need to QC the data you can:
```
scp -r /nearline/ctb-pbellec/preprocessed_data/ukbb.qc /PATH/TO/MY/DIR
cd /PATH/TO/MY/DIR/ukbb.qc
find . -name "*.tar.gz" -exec bash -c 'tar -xzvf "$0" -C "${0%/*}"; rm "$0"' {} \;
```
