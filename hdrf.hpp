//
// Created by muzongshen on 2021/9/24.
//

#pragma once

#include <string>
#include <fstream>
#include <vector>
#include <bitset>
#include "dense_bitset.hpp"
#include "util.hpp"
#include "partitioner.hpp"

using namespace std;

class HdrfPartitioner: public Partitioner{
private:
    ifstream fin;
    vid_t num_vertices;
    size_t num_edges;
    size_t filesize;

    // batch processing globals
    uint32_t num_batches;
    uint32_t num_edges_per_batch;
    int num_partitions;
    double lamda;

    vector<vid_t> degrees;
    uint64_t max_partition_load;

    //balance vertex partition distribute
    vector<vector<vid_t>> part_degrees; //each partition node degree
    vector<int> balance_vertex_distribute; //each node belongs to which unique partition

    vector<uint64_t> edge_load;
    vector<dense_bitset> vertex_partition_matrix;
    dense_bitset true_vids;
    uint64_t min_load = UINT64_MAX;
    uint64_t max_load = 0;
    const double epsilon = 1;
protected:
    void batch_hdrf(vector<edge_t> &edges);
    int find_max_score_partition_hdrf(edge_t& e);
    void update_vertex_partition_matrix(edge_t& e, int max_p);
    void update_min_max_load(int max_p);
    void batch_node_assign_neighbors(vector<edge_t> &edges);
    void read_and_do(string opt_name);
public:
    HdrfPartitioner(string file_name,string method, int pnum, int memsize, double balance_ratio, double balance_lambda, bool shuffle);
    void split();
};

