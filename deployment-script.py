import pandas as pd
import json
import subprocess
from kubeflow import pytorchjob

#Builds and pushes the container
def docker_process():
    gcrPath = "gcr.io/group5fairness/fltk"
    ##Prepare to build the dockerimage
    print("*********** Script: Preparing build for docker image ***********".format(gcrPath))
    commandDockerBuild = "DOCKER_BUILDKIT=1 docker build . --tag {}".format(gcrPath)
    subprocess.Popen(commandDockerBuild, shell=True, stdout = subprocess.PIPE).communicate()
    #print (buildOutput)

    print("*********** Script: Done building image. About to push to {} ***********".format(gcrPath))
    commandDockerPush = "docker push {}".format(gcrPath)
    subprocess.Popen(commandDockerPush, shell=True, stdout = subprocess.PIPE).communicate()
    print("*********** Script: Succesfuly pushed to {} ***********".format(gcrPath))

#Reads sign table and configures the example_cloud_experiments.json
def prepare_experiment_file():
    
    ##Translate sign table to appropriate values
    memoryValue = memoryTable[0] if (row["perc memory"] < 0) else memoryTable[1]
    cpuValue = cpuTable[0] if (row["perc cpu"] < 0) else cpuTable[1]
    nodesValue = nodesTable[0] if (row["# of nodes"] < 0) else nodesTable[1]
    groupsValue = groupsTable[0] if (row["# of groups"] < 0) else groupsTable[1]
    jobsPerGroupValue = jobsPerGroupTable[0] if (row["Jobs per group"] < 0) else jobsPerGroupTable[1]
    algorithmValue = algorithmTable[0] if (row["Algorithm"] < 0) else algorithmTable[1]
    # Opening JSON file
    f = open('configs/example_cloud_experiment.json',)
    # returns JSON object as a dictionary
    dictionary = json.load(f)
    #Alter dictionary
    dictionary["experiment"]["memory_per_job"] = memoryValue
    dictionary["experiment"]["cpu_per_job"] = cpuValue
    dictionary["experiment"]["number_of_groups"] = groupsValue
    dictionary["experiment"]["number_of_jobs_per_group"] = jobsPerGroupValue
    dictionary["experiment"]["scheduler"] = algorithmValue

    ##Write the dictionary as a json back to the file immidiately. 
    with open('example_cloud_experiment.json', 'w', encoding='utf-8') as f:
        json.dump(dictionary, f, ensure_ascii=False, indent=4)

#Simply performs the commands from the lab tutorial to kickstart the experiment (excluding extractor installment.)
def start_experiment():
    
    #Cd to /charts
    subprocess.Popen("cd charts", shell=True, stdout = subprocess.PIPE).communicate()
    #Make sure there is no orchestrator installed already by just uninstalling it anyway
    subprocess.Popen("helm uninstall orchestrator --namespace test", shell=True, stdout = subprocess.PIPE).communicate()
    print("*********** Script: Installing the orchestrator in the cluster...")
    subprocess.Popen("helm install orchestrator ./orchestrator --namespace test -f fltk-values.yaml", shell=True, stdout = subprocess.PIPE).communicate()
    print("*********** Script: Finished installing the orchestrator.***********")

#Uses the PyTorch lib to ensure that all pytorchjobs are indeed done. 
def wait_for_jobs():
    ##using this lib https://github.com/kubeflow/pytorch-operator/tree/master/sdk/python#documentation-for-api-endpoints
    pytorchjob_client = PyTorchJobClient()
    ##Gets all pytorch jobs in the namespace test (change if we will change the namespace...)
    allJobs = pytorchjob_client.get(namespace='test')
    for x in allJobs:
        #It should watch all jobs every 30 seconds, with a timeout of 600 seconds, untill they all reach eiter Succeeded or Failed.
        pytorchjob_client.wait_for_job(x, namespace='test', watch=True,  timeout_seconds=600)

#Saves data locally. Dropbox was being a b*tch.
def save_data():
    save_path = '/home'
    file_name = "test.txt"

    completeName = os.path.join(save_path, file_name)
    file1 = open(completeName, "w")
    file1.write("file information")
    file1.close()
    print("********Saved file to give path!")

#region sign-table-coefficients
## Allocate Values corresponding to sign table in .csv
## -1 corresponds to index 0
## 1 corresponds to index 1
memoryTable = [25, 100,]
cpuTable = [25, 100]
nodesTable = [1, 4]
groupsTable = [2, 8]
jobsPerGroupTable = [1, 10]
algorithmTable = ["random", "G5 Algorithm"]
##print(df)
#endregion
df = pd.read_csv("configs/2^k setup.csv", delimiter=';')
for index, row in df.iterrows():
    experimentId = row["Experiment"]
    print("*********** Script: Dealing with {} ***********".format(experimentId))
    prepare_experiment_file()
    docker_process()
    start_experiment()
    ###########TODO ##########
    wait_for_jobs() ##How do we know when we can start pulling data...?
    ##PullData() ##Where to get it from and how??
    ##SaveData() ##Where to put it?

    #get out of chart directorydirectory for the next run
    subprocess.Popen("cd ..", shell=True, stdout = subprocess.PIPE).communicate()
    input("Press Enter to move to the next experiment...")

