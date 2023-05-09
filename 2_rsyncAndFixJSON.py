import json
import subprocess
import os
from multiprocessing.pool import Pool
import time
from dotenv import load_dotenv

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
      
        
def fix_bids_datastructure(batch_subjects, scratch_path):
    with open(os.path.join(scratch_path,"ukbb","scripts","data","json_stats.json"), "r") as json_file:
        json_stats = json.load(json_file)
        print("noJSON: ",len(json_stats["noJSON"]), "validJSON: ",len(json_stats["validJSON"]), "wrongSliceTiming: ",len(json_stats["wrongSliceTiming"]))

    with open(os.path.join(scratch_path,"ukbb","scripts","data","json_stats_T1.json"), "r") as json_file:
        json_stats_T1 = json.load(json_file)
        print("noJSON: ",len(json_stats_T1["noJSON"]), "validJSON: ",len(json_stats_T1["validJSON"]))
    
    i = 0    
    tot = len(json_stats["noJSON"])+len(json_stats["wrongSliceTiming"])+len(json_stats["validJSON"])
    

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
    for subject in json_stats["noJSON"]:
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
     
if __name__ == "__main__":
    load_dotenv()
    multicore=True
    scratch_path=os.getenv('SCRATCH_PATH')
    bids_path=os.getenv('UKBB_BIDS_FOLDER')
    batch_size=int(os.getenv('BATCH_SIZE'))
    cluster_username_addr=os.getenv('CLUSTER_USERNAME_ADDR')
    
    rsync_batch=True
    fix_BIDS=True
    
    with open(os.path.join(scratch_path,"ukbb","scripts","data","json_stats.json"), "r") as json_file:
        json_stats = json.load(json_file)
        print("[fMRI stats] noJSON: ",len(json_stats["noJSON"]), "wrongSliceTiming: ",len(json_stats["wrongSliceTiming"]), "validJSON: ",len(json_stats["validJSON"]))
        print("[fMRI stats] total entries: ",len(json_stats["noJSON"])+len(json_stats["wrongSliceTiming"])+len(json_stats["validJSON"]))
    ukbb_subjects = json_stats["validJSON"] + json_stats["noJSON"]# + json_stats["wrongSliceTiming"]
    
    subjects_state_path = os.path.join(scratch_path,"ukbb","scripts","data","subjects_state.json")
    archived_subjects_path = os.path.join(scratch_path,"ukbb","scripts","data","archived_subjects.json")
    with open(subjects_state_path,"r") as json_file:
        subjects_state = json.load(json_file)
    with open(archived_subjects_path,"r") as json_file:
        archived_subjects = json.load(json_file)
    
    output_path = os.path.join(scratch_path,"ukbb","ukbb_bids")
    number_of_active_subject = int(subprocess.check_output(f"cd {output_path}; ls -l | wc -l", shell=True, text=True))-1
    
    print(f"There are already {number_of_active_subject} active subjects.")

    batch = []
    for subject in ukbb_subjects:
        if number_of_active_subject >= batch_size:
            break
        elif os.path.exists(os.path.join(output_path,subject)):
            batch.append(subject)
        elif subject in archived_subjects:
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

