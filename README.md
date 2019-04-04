# Class outlier.OutlierDetect

A parallel approach using the parallel, advanced, slicing and pmcod algorithms with the grid and metric partitioning for distance based outlier detection on streams.

Parameters:
**--input** <Input path of data>
**--treeInput** (Optional) <Input path for the creation of the tree in metric partitioning>
**--window** <window size>
**--slide** <slide size>
**--dataset** <dataset name for grid partitioning (works on stock, tao, fc, gauss)>
**--k** <number of neighbors>
**--range** <range for distance>
**--algorithm** <parallel,advanced,advanced_vp,slicing,pmcod>
**--part** (Optional) <Partitioning type - grid,metric>
**--VPcount** (Optional) <Number of elements for tree creation in metric partitioning>
**--parallelism** <Parallelism degree for Flink>

# Class multi_rk_param_outlier.OutlierDetect

A parallel approach using the pamcod, sop, pmcksky and psod algorithms with the metric partitioning for multi-parameter (application parameters) distance based outlier detection on streams.

Parameters:
**--input** <Input path of data>
**--treeInput** (Optional) <Input path for the creation of the tree in metric partitioning>
**--window** <window size>
**--slide** <slide size>
**--dataset** <dataset name for grid partitioning (works on stock, tao, fc, gauss)>
**--algorithm** <pamcod,ksky,pmcksky,psod>
**--part** (Optional) <Partitioning type - only metric>
**--VPcount** (Optional) <Number of elements for tree creation in metric partitioning>
**--parallelism** <Parallelism degree for Flink>
**--q** <Queries of k and R separated by ";" and "," (i.e. 50;0.45,40;0.35)>

# Class multi_rk_param_outlier.OutlierDetect

A parallel approach using the sop, pmcksky and psod algorithms with metric partitioning for multi-parameter (application and windowing parameters) distance based outlier detection on streams.

Parameters:
**--input** <Input path of data>
**--treeInput** (Optional) <Input path for the creation of the tree in metric partitioning>
**--window** <window size values separated with comma ",">
**--slide** <slide size values separated with comma ",">
**--r** <range values separated with comma ",">
**--k** <number of neighbors values separated with comma ",">
**--dataset** <dataset name for grid partitioning (works on stock, tao, fc, gauss)>
**--algorithm** <pamcod,ksky,pmcksky,psod>
**--part** (Optional) <Partitioning type - only metric>
**--VPcount** (Optional) <Number of elements for tree creation in metric partitioning>
**--parallelism** <Parallelism degree for Flink>