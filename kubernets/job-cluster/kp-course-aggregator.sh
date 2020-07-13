#!/bin/bash
export SUNBIRD_DATAPIPELINE_IMAGE=manjudr/kp-course-metrics-aggregator-fix1:1.0
export JOB_NAME=course-metrics-aggregator
export JOB_CLASSNAME=org.sunbird.kp.course.task.CourseMetricsAggregatorStreamTask
export AZURE_STORAGE_ACCOUNT=
export AZURE_STORAGE_SECRET=
export REST_SERVICE_PORT=30701
