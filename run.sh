#!/bin/bash
set -e

# Usage: ./run.sh <nodes_count> <partitionned> <type>
nodes_count=$1
partitionned=$2
type=$3 # "rdd" or "dataframe"

bucket_name="pagerank-homework"
cluster="pagerank-cluster"
data="gs://public_lddm_data/small_page_links.nt"
output="gs://$bucket_name/nodes-$nodes_count/partitionned-$partitionned"

# if project_id is not set, exit
if [ -z "$GOOGLE_CLOUD_PROJECT" ]; then
  echo "Please set the project id using 'gcloud config set project <PROJECT_ID>'"
  exit 1
fi

# Create bucket if not exists
exists=$(gsutil ls -b gs://$bucket_name 2>&1)
# Output if not exists: "BucketNotFoundException: 404 gs://pagerank-homework bucket does not exist."
if [[ $exists == *"BucketNotFoundException"* ]]; then
  gcloud storage buckets create $bucket_name --project="$GOOGLE_CLOUD_PROJECT" --default-storage-class=standard --location=europe-west1
fi

# Clean bucket
gsutil -m rm -r "$output"

# Move py scripts to bucket (overwrite if exists)
gsutil cp pagerank_"${type}".py gs://$bucket_name/pagerank_"${type}".py

#Create cluster
if [[ $nodes_count -ge 2 ]]; then
    gcloud dataproc clusters create ${cluster} \
        --enable-component-gateway --region europe-west1 \
        --zone europe-west1-c --master-machine-type n1-standard-4 \
        --master-boot-disk-size 20 --master-boot-disk-type pd-balanced \
        --num-workers "${nodes_count}" \
        --worker-machine-type n1-standard-4 --worker-boot-disk-size 20 \
        --worker-boot-disk-type pd-balanced \
        --image-version 2.2-debian12 --project "$GOOGLE_CLOUD_PROJECT"
else
    gcloud dataproc clusters create ${cluster} --single-node \
        --enable-component-gateway --region europe-west1 \
        --zone europe-west1-c --master-machine-type n1-standard-4 \
        --master-boot-disk-size 20 --master-boot-disk-type pd-balanced \
        --image-version 2.2-debian12 --project "$GOOGLE_CLOUD_PROJECT"
fi

# Start job
gcloud dataproc jobs submit pyspark --region europe-west1 \
    --cluster ${cluster} gs://$bucket_name/pagerank_"${type}".py \
    -- ${data} 3 "${partitionned}" "${output}"

gcloud dataproc clusters describe ${cluster} --region europe-west1
gcloud dataproc clusters delete ${cluster} --region europe-west1
