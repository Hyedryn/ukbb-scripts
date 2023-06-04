import os
import json
from dotenv import load_dotenv

if __name__ == "__main__":
    load_dotenv()
    scratch_path = os.getenv('SCRATCH_PATH')
    archive_index_path = os.path.join(scratch_path,"ukbb","scripts","data","archive_index.json")
    nearline_dir = os.path.join(scratch_path,"ukbb","nearline")
    md5List = [f for f in os.listdir(nearline_dir) if os.path.isfile(os.path.join(nearline_dir, f)) and f.split(".")[-1]=="md5"]
    
    archived_subjects_path = os.path.join(scratch_path,"ukbb","scripts","data","archived_subjects.json")
    with open(archived_subjects_path,"r") as json_file:
        archived_subjects = json.load(json_file)
        
    print(md5List)
    
    archive_index = {}
    
    totalSubject = []
    
    for md5 in md5List:
        with open(os.path.join(nearline_dir,md5)) as file:
            subjects = [line.rstrip().split("  ")[1].split("_")[0] for line in file]
        print(len(subjects))
        
        for sub in subjects:
            archive_index[sub] = md5.replace(".md5","")
            if sub not in archived_subjects:
                print(f"Subject {sub} not in archived_subjects!")
            if sub in totalSubject:
                print(f"Subject {sub} duplicated!")
        totalSubject.extend(subjects)

    print(len(totalSubject))
    
    with open(archive_index_path,"w") as json_file:
        json.dump(archive_index, json_file, indent=4)