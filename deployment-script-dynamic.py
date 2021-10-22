import time
import pandas as pd
import json
import subprocess


nodes = 4
jobs = 9
pipelines = 4
groups = 8

# Builds and pushes the container
def docker_process():
    gcrPath = "gcr.io/group5fairness/fltk"
    # Prepare to build the dockerimage
    print("Script: Preparing build for docker image".format(gcrPath))
    commandDockerBuild = "DOCKER_BUILDKIT=1 docker build . --tag {}".format(gcrPath)
    subprocess.Popen(commandDockerBuild, shell=True, stdout = subprocess.PIPE).communicate()

    print("Script: Done building image. About to push to {}".format(gcrPath))
    commandDockerPush = "docker push {}".format(gcrPath)
    subprocess.Popen(commandDockerPush, shell=True, stdout = subprocess.PIPE).communicate()
    print("Script: Succesfuly pushed to {}".format(gcrPath))


# Reads sign table and configures the example_cloud_experiments.json
def prepare_experiment_file(sche, r):
    print(sche, r)
    # Opening JSON file
    f = open('configs/example_cloud_experiment.json',)
    # returns JSON object as a dictionary
    dictionary = json.load(f)
    # Alter dictionary

    dictionary["experiment"]["repetition"] = r
    dictionary["experiment"]["static"] = False
    dictionary["experiment"]["scheduler"] = sche
    dictionary["experiment"]["nodes"] = nodes
    dictionary["experiment"]["number_of_groups"] = groups
    dictionary["experiment"]["number_of_jobs_per_group"] = jobs
    dictionary["experiment"]["pipelines"] = pipelines

    # Write the dictionary as a json back to the file immidiately.
    with open('configs/example_cloud_experiment.json', 'w', encoding='utf-8') as f:
        json.dump(dictionary, f, ensure_ascii=False, indent=4)


# Simply performs the commands from the lab tutorial to kickstart the experiment (excluding extractor installment.)
def start_experiment():
    print("Script: Installing the orchestrator in the cluster...")
    subprocess.Popen("cd charts && helm install orchestrator ./orchestrator --namespace test -f fltk-values.yaml", shell=True, stdout = subprocess.PIPE).communicate()
    print("Script: Finished installing the orchestrator.")


# Uses the PyTorch lib to ensure that all pytorchjobs are indeed done.
def wait_for_jobs():
    print("Script: Started waiting for fl-server to be done...")
    namespaceCommand = " kubectl config set-context --current --namespace=test"
    commandCheckFlServer = "kubectl get pods fl-server --no-headers -o custom-columns=\":status.phase\""
    while True:
        time.sleep(60)  # Wait 60 seconds before checking again.

        subprocess.Popen(namespaceCommand, shell=True, stdout = subprocess.PIPE).communicate()
        process = subprocess.run(commandCheckFlServer, capture_output=True, shell=True)
        stdout_as_str = process.stdout.decode("utf-8")
        ##TODO: I assume that the fl-server is in status Succeeded when it is done.
        if "Running" in stdout_as_str: 
            print("Script: fl-server is still running. Will try again in 60 seconds!")
        elif "Succeeded" in stdout_as_str :
            print("Script: fl-server done!")
            break
        elif "Failed" in stdout_as_str:
            print("Script: fl-server failed...")
            break


def end_experiment():
    print("Script: Uninstalling the orchestrator in the cluster...")

    subprocess.Popen("cd charts && helm uninstall orchestrator --namespace test", shell=True, stdout = subprocess.PIPE).communicate()

    namespaceCommand = " kubectl config set-context --current --namespace=test"
    commandCheckFlServer = "kubectl get pods fl-server --no-headers -o custom-columns=\":status.phase\""

    while True:
        print("Script: Waiting for the orchestrator to terminate...")

        time.sleep(5)

        subprocess.Popen(namespaceCommand, shell=True, stdout=subprocess.PIPE).communicate()
        process = subprocess.run(commandCheckFlServer, capture_output=True, shell=True)
        stdout_as_str = process.stdout.decode("utf-8")
        if len(stdout_as_str) == 0:
            break



def main():
    # region sign-table-coefficients
    # Allocate Values corresponding to sign table in .csv
    # -1 corresponds to index 0
    # 1 corresponds to index 1

    # Make sure the orchestrator gets killed
    end_experiment()

    count = 0
    for sche in ['fair', 'fifo', 'random']:
        for r in range(10, 20):
            print("Script: Dealing with {}".format(count))
            prepare_experiment_file(sche, r)
            docker_process()
            start_experiment()
            wait_for_jobs()
            # input("Press Enter to move to the next experiment...")

            end_experiment()
        count += 1



if __name__ == "__main__":
    main()
