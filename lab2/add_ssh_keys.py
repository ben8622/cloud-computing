import os
import sys

key_file_path = ""
project_name = ""

def main():
    if(len(key_file_path) == 0 or len(project_name) == 0):
        print("Not enough arguments.")
        exit()

    os.system(f"gcloud compute os-login ssh-keys add --key-file={key_file_path} --project={project_name}")

if __name__ == "__main__":
    for i, arg in enumerate(sys.argv):
        if(i == 1): key_file_path = arg
        if(i == 2): project_name = arg
    main()