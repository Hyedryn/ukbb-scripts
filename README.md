### Environment

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
git clone git@github.com:ccna-biomarkers/ukbb-preprocess-template.git $SCRATCH/datasets/ukbb
```

5. Install the timeserie extraction tool
```
git clone git@github.com:ccna-biomarkers/ccna_ts_extraction.git
module load python/3.8
python3 -m venv ~/.virtualenvs/ts_extraction
source ~/.virtualenvs/ts-extraction/bin/activate
git clone git@github.com:ccna-biomarkers/ccna_ts_extraction.git
python3 -m pip install ccna_ts_extraction/requirements.txt
deactivate
```

###  Data

Download ukbb zip files using ukbbfetch utility and place the archives in `$SCRATCH/ukbb_zip_files`

Download all templates needed by fmriprep:
```
python3 -c "from templateflow.api import get; get(['MNI152NLin2009cAsym', 'MNI152NLin6Asym', 'OASIS30ANTs', 'MNIPediatricAsym', 'MNIInfant'])"
```

Get the segmented difumo atlas in your $SCRATCH avalaible on beluga:
```
mkdir -p ${SCRATCH}/atlases
scp beluga.computecanada.ca:/nearline/ctb-pbellec/atlases/segmented_difumo_atlases_2022-02-03.tar.gz $SCRATCH/atlases/
tar -zxvf $SCRATCH/atlases/segmented_difumo_atlases_2022-02-03.tar.gz -C ${SCRATCH}/atlases
scp -r ${SCRATCH}/atlases/segmented_difumo_atlases/* ${SCRATCH}/atlases && rm -r ${SCRATCH}/atlases/segmented_difumo_atlases && rm $SCRATCH/atlases/segmented_difumo_atlases_2022-02-03.tar.gz
```

### Usage

Submit a preprocessing job for one participant `PARTICIPANT_ID` with:

```
$HOME/ukbb_scripts/ukbb_preprocess.bash PARTICIPANT_ID
```
