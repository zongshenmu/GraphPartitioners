//
// Created by muzongshen on 2021/9/23.
//

#pragma once

#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <random>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>

#include "dense_bitset.hpp"
#include "util.hpp"
#include "partitioner.hpp"


using namespace  std;

class DbhPartitioner : public Partitioner
{
private:
    vid_t num_vertices;
    size_t num_edges;
    size_t filesize;

    int p;
    ifstream fin;

    std::vector<vid_t> degrees;

    uint32_t num_batches;
    uint32_t num_edges_per_batch;

    dense_bitset true_vids;;
    std::vector<dense_bitset> is_mirrors;
    std::vector<size_t> counter;
    vector<vector<vid_t>> part_degrees;
    vector<int> balance_vertex_distribute;

protected:
    void read_and_do(string opt_name);
    void batch_dbh(vector<edge_t> &edges);
    void batch_node_assignment(vector<edge_t> &edges); 
public:
    DbhPartitioner(string basefilename, string method, int pnum, int memsize, bool shuffle);
    void split();
};
