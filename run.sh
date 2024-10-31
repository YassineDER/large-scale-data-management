#!/bin/bash

nodes_count=$1
type=$2 # "rdd" or "dataframe"
service_account=$3

project_id=${GOOGLE_CLOUD_PROJECT}
bucket_name="gs://pagerank-homework"
cluster="pagerank-cluster-n$nodes_count-$type"
data="gs://public_lddm_data/small_page_links.nt"

# if project_id is not set, exit
echo "Checking project id if set..."
if [ -z "$project_id" ]; then
  echo "Please set the project id using 'gcloud config set project <PROJECT_ID>' or in GOOGLE_CLOUD_PROJECT environment variable."
  exit 1
fi

# Checking if service account is set
if [ -z "$service_account" ]; then
  echo "Service account not set"
  service_account=$(gcloud compute project-info describe --format="value(defaultServiceAccount)" --project="$project_id")
  echo "using Default Service account: $service_account"
  if [ $service_account == "527092413767-compute@developer.gserviceaccount.com" ]; then
    # My default service account somehow does not exists in my project
    service_account="owner-318@tinypet-404519.iam.gserviceaccount.com"
  fi
fi

# Check if service account exists in google cloud (even the default one could not exist)
exists=$(gcloud iam service-accounts describe $service_account --format="value(displayName)" 2>&1)
if [[ $exists == *"ERROR"* ]]; then
  echo "Service account not found, please provide a valid service account with the right permissions"
  exit 1
fi

# Create bucket if not exists
exists=$(gsutil ls -b $bucket_name 2>&1)
if [[ $exists == *"BucketNotFoundException"* ]]; then
  echo "Bucket does not exist, creating it..."
  gcloud storage buckets create $bucket_name --project="$project_id" \
  --default-storage-class=standard --location=europe-west1 --no-public-access-prevention
fi

partitionned="true"
output="$bucket_name/nodes-$nodes_count/partitionned-$partitionned"

# Clean bucket
gsutil -m rm -r "$output"
echo "previous output cleaned from bucket"

# Move py script to bucket (overwrite if exists)
script="pagerank_${type}.py"
gcloud storage cp $script $bucket_name
echo "script moved to bucket"

#Create cluster
echo "Creating cluster $cluster with $nodes_count nodes..."
# Check if cluster exists
cluster_exists=$(gcloud dataproc clusters list --region europe-west1 --filter="clusterName=${cluster}" --format="value(clusterName)")
if [[ ! -n "$cluster_exists" ]]; then
    if [[ $nodes_count -ge 2 ]]; then
        gcloud dataproc clusters create ${cluster} \
            --enable-component-gateway --region europe-west1 \
            --zone europe-west1-c --master-machine-type n1-standard-4 \
            --master-boot-disk-size 30 --master-boot-disk-type pd-balanced \
            --num-workers "${nodes_count}" --public-ip-address \
            --worker-machine-type n1-standard-4 --worker-boot-disk-size 30 \
            --worker-boot-disk-type pd-balanced --service-account="$service_account" \
            --image-version 2.2-debian12 --project "$project_id"
    else
        gcloud dataproc clusters create ${cluster} --single-node \
            --enable-component-gateway --region europe-west1 \
            --zone europe-west1-c --master-machine-type n1-standard-4 \
            --master-boot-disk-size 30 --master-boot-disk-type pd-balanced \
            --image-version 2.2-debian12 --project "$project_id" \
            --service-account="$service_account" --public-ip-address
    fi
else
    echo "Cluster ${cluster} already exists, skipping creation."
fi

# Start job (3 iterations)
gcloud dataproc jobs submit pyspark --region europe-west1 \
    --cluster ${cluster} $bucket_name/$script \
    -- ${data} 3 "${partitionned}" "${output}"

partitionned="false"
output="$bucket_name/nodes-$nodes_count/partitionned-$partitionned"

# Clean bucket
echo "Cleaning bucket from previous similar output..."
gsutil -m rm -r "$output"

# Start job (3 iterations)
gcloud dataproc jobs submit pyspark --region europe-west1 \
    --cluster ${cluster} $bucket_name/$script \
    -- ${data} 3 "${partitionned}" "${output}"

gcloud dataproc clusters delete ${cluster} --region europe-west1 --quiet
