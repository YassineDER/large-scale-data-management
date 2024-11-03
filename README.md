# PageRank Homework

## Getting Started

The scripts must be run on a Google Cloud Shell with the following commands:

```bash
chmod +x run.sh
make -C . all_rdd_small
make -C . all_rdd_large
```

## Makefile behavior

The Makefile runs the `run.sh` script with the following parameters, for each case:

```bash
./run.sh <nodes_count> \<type> \<data_type> \<service_account>
``` 
Where:
- nodes_count: number of nodes to use in DataProc
- type: "rdd" or "dataframe"
- data_type: "small" to process the `small_page_links.nt` file, "large" to process the `page_links_en.nt.bz2` file
- service_account: service account to use for DataProc in email format (optional, default is compute engine service account)

## Results: `small_page_links.nt`

The results are based on **3 iterations** in the PageRank algorithm, and the number of partitions is set to **4**.


| Nodes | RDD - Time without partitioning | RDD - Time with partitioning |
|-------|---------------------------------|------------------------------|
| 1     | 44s                             | 54s                          |
| 2     | 29s                             | 39s                          |
| 4     | 32s                             | 40s                          |

The entity with the highest PageRank is

| Nodes | DataFrame - Time without partitioning | DataFrame - Time with partitioning |
|-------|---------------------------------------|------------------------------------|


```