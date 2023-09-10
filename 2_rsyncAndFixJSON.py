import json
import subprocess
import os
from multiprocessing.pool import Pool
import time
from dotenv import load_dotenv
import zipfile
from pathlib import Path
import nibabel as nb

def rsync_subject(subject_id, bids_path, output_folder, scratch_path, cluster_username_addr=""):
    """Rsync a subject from the BIDS dataset to the output folder."""
    if cluster_username_addr == None:
        cluster_username_addr = ""
    subject_path = os.path.join(bids_path, subject_id)
    output_path = os.path.join(output_folder, subject_id)
    cmd = 'rsync -arl {}{}/ {}'.format(cluster_username_addr, subject_path, output_path)
    std_output = open(os.path.join(scratch_path,"ukbb","scripts","data","rsync.log"), "a")
    subprocess.call(cmd, shell=True)#, stdout=std_output)
    print("Subject {} rsynced".format(subject_id), file=std_output)
    std_output.close()
    
    
    return subject_id
      
      
def sync_freesurfer_subject(subject_id, freesurfer_path, scratch_path):
    sub = subject_id.split("-")[1]
    subject_path = os.path.join(freesurfer_path, f"{sub}_20263_2_0.zip")
    output_path = os.path.join(scratch_path,"ukbb","ukbb_freesurfer",subject_id + "_freesurfer.zip")
    if not os.path.exists(output_path):
        if os.path.exists(subject_path):
            cmd = 'rsync -arl {} {}'.format(subject_path, output_path)
            subprocess.call(cmd, shell=True)
            #output_path_tmp = os.path.join(scratch_path,"ukbb","ukbb_freesurfer_tmp",subject_id)
            #os.makedirs(output_path, exist_ok=True)
            #os.makedirs(output_path_tmp, exist_ok=True)
            #with zipfile.ZipFile(subject_path, 'r') as zip_ref:
            #    zip_ref.extractall(output_path_tmp)
            #os.rename(os.path.join(output_path_tmp,"FreeSurfer"),output_path)
        else:
            print("No precomputed freesurfer output! More time needed for fmriprep!!")
        
    return subject_id
        
def fix_bids_datastructure(batch_subjects, scratch_path):
    with open(os.path.join(scratch_path,"ukbb","scripts","data","json_stats.json"), "r") as json_file:
        json_stats = json.load(json_file)
        print("noJSON: ",len(json_stats["noJSON"]), "validJSON: ",len(json_stats["validJSON"]), "wrongSliceTiming: ",len(json_stats["wrongSliceTiming"]))

    with open(os.path.join(scratch_path,"ukbb","scripts","data","json_stats_T1.json"), "r") as json_file:
        json_stats_T1 = json.load(json_file)
        print("noJSON: ",len(json_stats_T1["noJSON"]), "validJSON: ",len(json_stats_T1["validJSON"]))
    
    i = 0    
    tot = len(json_stats["noJSON"])+len(json_stats["wrongSliceTiming"])+len(json_stats["validJSON"])
    

    missing_json = []
    for subject in json_stats["wrongSliceTiming"] + json_stats["validJSON"]:
        if os.path.exists(os.path.join(scratch_path,"ukbb","ukbb_bids", subject)):
            json_path = os.path.join(scratch_path,"ukbb","ukbb_bids", subject,"func",subject+"_task-rest_bold.json")
            if os.path.exists(json_path):
                with open(json_path) as json_file:
                    json_data = json.load(json_file)
                if "SliceTiming" in json_data.keys():
                    del json_data["SliceTiming"]
                json_data["TaskName"] = "rest"
                with open(json_path, "w") as json_file:
                    json.dump(json_data, json_file, indent=4)
            else:
                print("ERROR: no json for subject {}".format(subject))
                missing_json.append(subject)

            json_sbref_path = os.path.join(scratch_path,"ukbb","ukbb_bids", subject, "func", subject + "_task-rest_sbref.json")
            if os.path.exists(json_sbref_path):
                with open(json_sbref_path) as json_file:
                    json_data = json.load(json_file)
                if "SliceTiming" in json_data.keys():
                    del json_data["SliceTiming"]
                json_data["TaskName"] = "rest"
                with open(json_sbref_path, "w") as json_file:
                    json.dump(json_data, json_file, indent=4)
        elif subject in batch_subjects:
            print("ERROR subject", subject, "not found (fmri_JSON)")
        if i % 500 == 0:
            print("[fMRI] ",i,"/",tot, "subjects processed (",100*(i/tot) ,"%)")
        i=i+1
        

    fMRI_NOJSON = {
        "Manufacturer": "Siemens",
        "ManufacturersModelName": "Skyra",
        "ImageType": ["ORIGINAL", "PRIMARY", "M", "MB", "ND", "MOSAI"],
        "MagneticFieldStrength": 3,
        "FlipAngle": 51,
        "EchoTime": 0.0424,
        "RepetitionTime": 0.735,
        "EffectiveEchoSpacing": 0.000639989,
        "PhaseEncodingDirection": "j-",
        "TaskName": "rest"
    }
    fMRI_sbref_NOJSON = {
        "Manufacturer": "Siemens",
        "ManufacturersModelName": "Skyra",
        "ImageType": ["ORIGINAL", "PRIMARY", "M", "ND", "MOSAIC"],
        "MagneticFieldStrength": 3,
        "FlipAngle": 51,
        "EchoTime": 0.0424,
        "RepetitionTime": 0.735,
        "EffectiveEchoSpacing": 0.000639989,
        "PhaseEncodingDirection": "j-",
        "TaskName": "rest"
    }
    for subject in json_stats["noJSON"] + missing_json:
        if os.path.exists(os.path.join(scratch_path,"ukbb","ukbb_bids", subject)):
            json_path = os.path.join(scratch_path,"ukbb","ukbb_bids", subject,"func",subject+"_task-rest_bold.json")
            with open(json_path, "w") as json_file:
                json.dump(fMRI_NOJSON, json_file, indent=4)

            json_sbref_path = os.path.join(scratch_path,"ukbb","ukbb_bids", subject, "func", subject + "_task-rest_sbref.json")
            with open(json_sbref_path, "w") as json_file:
                json.dump(fMRI_sbref_NOJSON, json_file, indent=4)
        elif subject in batch_subjects:
            print("ERROR subject", subject, "not found (fMRI_NOJSON)")
        if i % 500 == 0:
            print("[fMRI] ",i,"/",tot, "subjects processed (",100*(i/tot) ,"%)")
        i=i+1
        
    T1_NOJSON = {
        "Manufacturer": "Siemens",
        "ManufacturersModelName": "Skyra",
        "ImageType": ["ORIGINAL", "PRIMARY", "M", "ND", "NORM"],
        "MagneticFieldStrength": 3,
        "FlipAngle": 8,
        "EchoTime": 0.00201,
        "RepetitionTime": 2,
        "PhaseEncodingDirection": "i-"
    }
    i = 0    
    tot = len(json_stats_T1["noJSON"])
    for subject in json_stats_T1["noJSON"]:
        if os.path.exists(os.path.join(scratch_path,"ukbb","ukbb_bids", subject)):
            json_path = os.path.join(scratch_path,"ukbb","ukbb_bids", subject,"anat",subject+"_T1w.json")
            with open(json_path, "w") as json_file:
                json.dump(T1_NOJSON, json_file, indent=4)
        elif subject in batch_subjects:
            print("ERROR subject", subject, "not found (T1_NOJSON)")
            
        if i % 500 == 0:
            print("[T1] ",i,"/",len(json_stats_T1["noJSON"]), "subjects processed (",100*(i/len(json_stats_T1["noJSON"])) ,"%)")
        i=i+1
     
     
def fix_nifti_header(subject, scratch_path):
    def set_xyzt_units(img, xyz='mm', t='sec'):
        header = img.header.copy()
        header.set_xyzt_units(xyz=xyz, t=t)
        return img.__class__(img.get_fdata().copy(), img.affine, header)
        
    def set_dim_info(img, slice=3,freq=1,phase=2):
        header = img.header.copy()
        header.set_dim_info(slice=3,freq=1,phase=2)
        return img.__class__(img.get_fdata().copy(), img.affine, header)

    def fixer(img_path):
        if os.path.exists(img_path):
            fixed_img = nb.load(img_path)
            fix = False
            if fixed_img.header.get_xyzt_units() == ('unknown', 'unknown'):
                print("Fixed xyzt units",img_path)
                fixed_img = set_xyzt_units(fixed_img)
                fix = True
            #if fixed_img.header.get_dim_info() == (None, None, None):
            #    print("Fixed dim info")
            #    fixed_img = set_xyzt_units(fixed_img)
            #    fix = True
            if fix:
                fixed_img.to_filename(img_path)
                print("Done")

    bold_path = os.path.join(scratch_path,"ukbb","ukbb_bids", subject,"func",subject+"_task-rest_bold.nii.gz")
    sbref_path = os.path.join(scratch_path,"ukbb","ukbb_bids", subject,"func",subject+"_task-rest_sbref.nii.gz")
    
    fixer(bold_path)
    fixer(sbref_path)
    


if __name__ == "__main__":
    load_dotenv()
    multicore=True
    scratch_path=os.getenv('SCRATCH_PATH')
    bids_path=os.getenv('UKBB_BIDS_FOLDER')
    freesurfer_path=os.getenv('UKBB_FREESURFER_FOLDER')
    batch_size=int(os.getenv('BATCH_SIZE'))
    cluster_username_addr=os.getenv('CLUSTER_USERNAME_ADDR')
    complementary_cluster_name = os.getenv('COMPLEMENTARY_CLUSTER_NAME')
    complementary_cluster_login = os.getenv('COMPLEMENTARY_CLUSTER_LOGIN')
    outside_cluster_name = os.getenv('OUTSIDE_CLUSTER_NAME')
    outside_cluster_login = os.getenv('OUTSIDE_CLUSTER_LOGIN')
    
    rsync_batch=False
    fix_BIDS=False
    fix_header=True
    
    with open(os.path.join(scratch_path,"ukbb","scripts","data","json_stats.json"), "r") as json_file:
        json_stats = json.load(json_file)
        print("[fMRI stats] noJSON: ",len(json_stats["noJSON"]), "wrongSliceTiming: ",len(json_stats["wrongSliceTiming"]), "validJSON: ",len(json_stats["validJSON"]))
        print("[fMRI stats] total entries: ",len(json_stats["noJSON"])+len(json_stats["wrongSliceTiming"])+len(json_stats["validJSON"]))
    ukbb_subjects = json_stats["validJSON"] + json_stats["noJSON"] + json_stats["wrongSliceTiming"]
    ukbb_subjects.reverse()
    subjects_state_path = os.path.join(scratch_path,"ukbb","scripts","data","subjects_state.json")
    archived_subjects_path = os.path.join(scratch_path,"ukbb","scripts","data","archived_subjects.json")
    with open(subjects_state_path,"r") as json_file:
        subjects_state = json.load(json_file)
    with open(archived_subjects_path,"r") as json_file:
        archived_subjects = json.load(json_file)
    
    output_path = os.path.join(scratch_path,"ukbb","ukbb_bids")
    number_of_active_subject = int(subprocess.check_output(f"cd {output_path}; ls -l | wc -l", shell=True, text=True))-1
    
    print(f"There are already {number_of_active_subject} active subjects.")
    
    active_subject_cmd = subprocess.check_output(f"rsync -az {complementary_cluster_login} {scratch_path}/ukbb/scripts/data/active_subjects_{complementary_cluster_name}.json", shell=True, text=True)
    print(active_subject_cmd)
<<<<<<< Updated upstream

=======
    
>>>>>>> Stashed changes
    active_subject_cmd = subprocess.check_output(f"rsync -az {outside_cluster_login} {scratch_path}/ukbb/scripts/data/active_subjects_{outside_cluster_name}.json", shell=True, text=True)
    print(active_subject_cmd)
    
    with open(f"{scratch_path}/ukbb/scripts/data/active_subjects_{complementary_cluster_name}.json", "r") as json_file:
        active_subject = json.load(json_file)
    print(f"Number of active subjects on {complementary_cluster_name} cluster: ",len(active_subject))
    
    with open(f"{scratch_path}/ukbb/scripts/data/active_subjects_{outside_cluster_name}.json", "r") as json_file:
        active_subject_outside = json.load(json_file)
    print(f"Number of active subjects on {outside_cluster_name} cluster: ",len(active_subject_outside))

    with open(f"{scratch_path}/ukbb/scripts/data/active_subjects_{outside_cluster_name}.json", "r") as json_file:
        active_subject_outside = json.load(json_file)
    print(f"Number of active subjects on {outside_cluster_name} cluster: ",len(active_subject_outside))
    
    batch = []
    for subject in ukbb_subjects:
        if number_of_active_subject >= batch_size:
            break
        elif os.path.exists(os.path.join(output_path,subject)):
            batch.append(subject)
        elif subject in archived_subjects:
            pass
        elif subject in active_subject:
            pass
        elif subject in active_subject_outside:
            pass
        elif subject in subjects_state:
            print(f"[Warning] Subject {subject} in subject_state and not archived but no input bids dataset found!")
            batch.append(subject)
            number_of_active_subject += 1
        else:
            batch.append(subject)
            number_of_active_subject += 1
    
    
    #####################################################
    #                      RSYNC                        #
    #####################################################
    
    if rsync_batch:
        print("Starting rsync of {} subjects among {} subjects.".format(len(batch),len(ukbb_subjects)))
        if multicore:
            i=0
            print("Starting freesurfer sync")
            with Pool(50) as pool:
                items = [(subject, freesurfer_path, scratch_path) for subject in batch]
                for subject in pool.starmap(sync_freesurfer_subject, items):
                    print("syncing freesurfer subject: ", subject, "(",i,"/",batch_size,")", "(",100*(i/batch_size),"%)")
                    i += 1
            print("Starting rsync")
            i = 0
            with Pool(50) as pool:
                items = [(subject, bids_path, output_path, scratch_path, cluster_username_addr) for subject in batch]
                for subject in pool.starmap(rsync_subject, items):
                    print("rsyncing subject: ", subject, "(",i,"/",batch_size,")", "(",100*(i/batch_size),"%)")
                    i += 1
        else:
            i = 0
            for subject in batch:
                print("rsyncing subject: ", subject, "(",i,"/",batch_size,")", "(",100*(i/batch_size),"%)")
                rsync_subject(subject, bids_path, output_path, scratch_path, cluster_username_addr)
                
                #Sync freesurfer
                print("Sync freesurfer")
                sync_freesurfer_subject(subject, freesurfer_path, scratch_path)
                
                i += 1
  
    #####################################################
    #               BIDS datastructure fix              #
    #####################################################
    
    effective_batch = []
    for sub in os.listdir(output_path):
        if sub in ukbb_subjects:
            effective_batch.append(sub)
    effective_batch_size = len(effective_batch)
    
    if fix_BIDS:
        print("Fixing BIDS datastructure for effective batch")
        fix_bids_datastructure(effective_batch,scratch_path=scratch_path)
        
    if fix_header:
        print("Fixing BIDS header for effective batch")
        i = 0
        for sub in effective_batch:
            fix_nifti_header(sub, scratch_path)
            if i % int(effective_batch_size/200) == 0:
                print(i,"/",effective_batch_size)
            i += 1
            

