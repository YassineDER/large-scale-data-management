all_rdd: 1_node_small_links_rdd 2_node_small_links_rdd 4_node_small_links

1_node_small_links_rdd:
	@bash run.sh 1 "rdd"

2_node_small_links_rdd:
	@bash run.sh 2 "rdd"

4_node_small_links:
	@bash run.sh 4 "rdd"

all_dataframe: 1_node_small_links_dataframe 2_node_small_links_dataframe 4_node_small_links_dataframe

1_node_small_links_dataframe:
	@bash run.sh 1 "dataframe"

2_node_small_links_dataframe:
	@bash run.sh 2 "dataframe"

4_node_small_links_dataframe:
	@bash run.sh 4 "dataframe"