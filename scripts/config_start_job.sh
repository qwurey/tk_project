# produce uuid
jobid=$(uuidgen)

# config job yaml
echo $jobid
cp ./template/tf_job.template ./template/tf_job.yaml
sed -i 's/{{job-id}}/'$jobid'/' ./template/tf_job.yaml
mv ./template/tf_job.yaml ./jobs/

# config delete_jobs.sh
cp ./template/delete_all.template ./template/delete_all.sh
sed -i 's/{{job-id}}/'$jobid'/' ./template/delete_all.sh
mv ./template/delete_all.sh ./scripts/


# start job and svc
kubectl create -f ./jobs/tf_job.yaml
