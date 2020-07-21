#!/bin/bash
export SUNBIRD_DATAPIPELINE_IMAGE=manjudr/dev-kp-stream-jobs:1.0.1
export JOB_NAME=relation-cache-updater-stream-job
export JOB_CLASSNAME=org.sunbird.job.task.RelationCacheUpdaterStreamTask
export AZURE_STORAGE_ACCOUNT=
export AZURE_STORAGE_SECRET=
export REST_SERVICE_PORT=30701
