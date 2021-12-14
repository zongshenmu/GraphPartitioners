## GraphPartitioner

This repo includes some classical graph partitioning methods for large-scale graph datastes. Graph Partitioning can roughly be categoried into vertex and edge partitioning. For vertex partitioning, it evenly distributes vertices to multiple workers each of which maintains a consistent partial state of the graph. And vice versa for edge partitioing. For more details please refer to the [survey](!https://dl.acm.org/doi/10.14778/3236187.3236208).

### Vertex Partitioning
The METIS is the most popular algorithm for vertex partitioning. And it has been employed in [DGL](!https://arxiv.org/abs/2010.05337) and [PGL](!https://github.com/PaddlePaddle/PGL/tree/add_3rd_party_metis/pgl). We implement the streaming vertex partitioning, LDG and Fennel. There are few public codes for these methods and our codes may not the most efficency one. You can run on small graph datastes.

### Edge Partitioning
We reproduce the NE partitioner and streaming methods (DBH and HDRF). 
### Prepare
You can employ your own datasets on above partitioners. The data format needs as beblow:
```
# ids must start from 0
# the graph is undirected and only list one directional edge
src_id tgt_id
```

some args in the `main.cpp`:
```
int pnum=150;
int memsize=4096;
string method="fennel";
double lambda=1.1;
double balance_ratio=1.05;
string edgename="/data0/mzs/Code/MPGraph/data/products/walks.txt";
```
pnum is the partitioned chunks, memsize is the memory size for edge  streaming methods, method is the choosed partitioner, lambda is for HDRF, balance_ratio is the edge counts deviating from the average, edgename is the absolute path for graph datasets.

Make and run the `main.cpp`. The output file is the partitioned results and saved in ``{edgename}.edgepart.{pnum}``.