kubectl delete job.batch/users-cache-updater-job-jobmanager
kubectl delete replicaset.apps/users-cache-updater-job-taskmanager-786b4dbcf
kubectl delete deployment.apps/users-cache-updater-job-taskmanager
kubectl delete daemonset.apps/svclb-users-cache-updater-job-jobmanager-rest
kubectl delete daemonset.apps/svclb-users-cache-updater-job-jobmanager-rest
kubectl delete service/users-cache-updater-job-jobmanager
kubectl delete service/users-cache-updater-job-jobmanager-rest