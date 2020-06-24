#!/bin/bash
export SUNBIRD_DATAPIPELINE_IMAGE=manjudr/course-agg-flink-job:1.0
export JOB_NAME=course-agg-job
export JOB_CLASSNAME=org.sunbird.dp.assessment.task.CourseAggStreamTask
export AZURE_STORAGE_ACCOUNT=
export AZURE_STORAGE_SECRET=
export REST_SERVICE_PORT=30508