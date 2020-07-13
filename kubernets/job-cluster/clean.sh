kubectl delete pod/course-metrics-aggregator-jobmanager-tc6fc
kubectl delete pod/course-metrics-aggregator-taskmanager-855cc88959-gmttk
kubectl delete deployment.apps/course-metrics-aggregator-taskmanager
kubectl delete job.batch/course-metrics-aggregator-jobmanager
kubectl delete replicaset.apps/course-metrics-aggregator-taskmanager-855cc88959
kubectl delete service/course-agg-job-jobmanager
kubectl delete service/course-agg-job-jobmanager-rest

