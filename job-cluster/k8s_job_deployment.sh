#!/bin/bash
# Source the env properties script for each job
# Comment or Uncomment depending upon the job you are deploying
# Create another script if you are working on new job
source ./user_cache_updater.sh
# source ./set_preprocessor_env.sh
# source ./set_druid_validator_env.sh
# source ./set_denorm_env.sh
envsubst <flink-configuration-configmap.yaml | kubectl create -f -
# This is used for communication between JobManager and TaskManager
envsubst <job-cluster-service.yaml | kubectl create -f -
# This is used for accessing the Flink Web UI
envsubst <job-cluster-restservice.yaml | kubectl create -f -
# Instantiate JobManager
envsubst <job-cluster-jobmanager.yaml | kubectl create -f -
# Instantiate TaskManager
envsubst <job-cluster-taskmanager.yaml | kubectl create -f -

kubectl get all