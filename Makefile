all_rdd_small: 1_node_small_rdd 2_node_small_rdd 4_node_small_rdd

1_node_small_rdd:
	@bash run.sh 1 "rdd" "small"

2_node_small_rdd:
	@bash run.sh 2 "rdd" "small"

4_node_small_rdd:
	@bash run.sh 4 "rdd" "small"



all_dataframe_small: 1_node_small_dataframe 2_node_small_dataframe 4_node_small_dataframe

1_node_small_dataframe:
	@bash run.sh 1 "dataframe" "small"

2_node_small_dataframe:
	@bash run.sh 2 "dataframe" "small"

4_node_small_dataframe:
	@bash run.sh 4 "dataframe" "small"