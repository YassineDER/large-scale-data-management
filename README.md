# PageRank Homework

## Getting Started

The scripts must be run on a Google Cloud Shell with the following commands:

```bash
chmod +x run.sh
make -C . all_rdd
make -C . all_dataframe
```

## Makefile behavior

The Makefile runs the `run.sh` script with the following parameters, for each case:

```bash
./run.sh <nodes_count> \<type> <service_account>
``` 
Where:
- nodes_count: number of nodes to use in DataProc
- type: "rdd" or "dataframe"
- service_account: service account to use for DataProc in email format (optional, default is compute engine service account)