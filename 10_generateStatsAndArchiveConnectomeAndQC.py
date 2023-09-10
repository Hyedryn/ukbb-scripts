import json
import subprocess
import os
from multiprocessing.pool import Pool
import time
from dotenv import load_dotenv
from pathlib import Path
import pandas as pd
import shutil

if __name__ == "__main__":
    load_dotenv()
    scratch_path=os.getenv('SCRATCH_PATH')
    nearline_path = os.getenv('UKBB_NEARLINE_ARCHIVE_FOLDER')
    email=os.getenv('SLURM_EMAIL')
    
    md5_list = []
    for dir in os.listdir(nearline_path):
        if "fmriprep_batch_" in dir and ".tar" in dir:
            md5_list.append(str(dir).split("_")[2].split(".")[0])
            
    # Generate Full QC file
    print("1. Generate Full QC file")
    task="rest"
    output_dir = os.path.join(scratch_path, "ukbb", "auto_qc")
    os.remove(os.path.join(output_dir, f"task-{task}_report.tsv"))
    for md5 in md5_list:
        input_tsv = os.path.join(scratch_path, "ukbb", "auto_qc", md5, "output_qc", f"task-{task}_report.tsv")
        metrics = pd.read_table(input_tsv)
        if os.path.exists(os.path.join(output_dir, f"task-{task}_report.tsv")):
            metrics.to_csv(os.path.join(output_dir,f"task-{task}_report.tsv"), sep="\t", mode='a', index=True, header=False)
        else:
            metrics.to_csv(os.path.join(output_dir, f"task-{task}_report.tsv"), sep="\t")
                
    print("2. QC stats")
    
    UKBB_QC = pd.read_table(os.path.join(output_dir, f"task-{task}_report.tsv"))
    #UKBB_QC.drop_duplicates(subset='participant_id', keep="first")
    UKBB_QC.to_csv(os.path.join(output_dir, f"task-{task}_report.tsv"), sep="\t")
    
    print(UKBB_QC.describe(),"\n\n")
    
    print(f"Functional QC stats:", UKBB_QC.pass_func_qc.value_counts(),"\n\n")
    print(f"Anatomical QC stats:", UKBB_QC.pass_anat_qc.value_counts(),"\n\n")
    print(f"All QC stats:", UKBB_QC.pass_all_qc.value_counts(),"\n\n")
    
    process_connectome=False
    if process_connectome:
        print("3. Divide Connectome By atlas type")
        
        connectome_folder = os.path.join(scratch_path, "ukbb", "connectome")
        for md5 in md5_list:
            if os.path.exists(os.path.join(connectome_folder, md5)):
                output_folder_DiFuMo = os.path.join(connectome_folder,"archive","DiFuMo", md5)
                os.makedirs(output_folder_DiFuMo, exist_ok = True)
                output_folder_MIST = os.path.join(connectome_folder,"archive", "MIST", md5)
                os.makedirs(output_folder_MIST, exist_ok = True)
                output_folder_Schaefer20187Networks = os.path.join(connectome_folder,"archive", "Schaefer20187Networks", md5)
                os.makedirs(output_folder_Schaefer20187Networks, exist_ok = True)
                
                for i in range(0,40):
                    connectome_dir = os.path.join(connectome_folder, md5, i, "output_connectome")
                    
                    duplicate_connectomes = [idx for idx, val in enumerate(os.listdir(connectome_dir)) if "_1.h5" in val]
                    for elem in duplicate_connectomes:
                        first_elem = elem.replace("_1.h5", ".h5")
                        if os.path.exists(os.path.join(connectome_dir,first_elem)):
                            os.remove(os.path.join(connectome_dir,first_elem))
                            os.rename(os.path.join(connectome_dir, elem) , os.path.join(connectome_dir,first_elem))
                            
                    connectomes = os.listdir(connectome_dir)
                    
                    for connectome in connectomes:
                        if "DiFuMo" in connectome:
                            #Copy to DiFuMo
                            shutil.copy(os.path.join(connectome_dir, connectome), output_folder_DiFuMo)
                        elif "Schaefer20187Networks" in connectome:
                            #Copy to Schaefer20187Networks
                            shutil.copy(os.path.join(connectome_dir, connectome), output_folder_Schaefer20187Networks)
                        elif "MIST" in connectome:
                            #Copy to MIST
                            shutil.copy(os.path.join(connectome_dir, connectome), output_folder_MIST)
                        else:
                            print("Unknown file:", connectome)
                