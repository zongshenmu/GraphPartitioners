//
// Created by muzongshen on 2021/9/30.
//

#ifndef GRAPHPARTITIONING_FENNEL_HPP
#define GRAPHPARTITIONING_FENNEL_HPP


#include <string>
#include <iostream>
#include <fstream>
#include <map>
#include <unordered_set>
#include <set>
#include <vector>
#include "../partitioner.hpp"
#include "../dense_bitset.hpp"
#include "../util.hpp"
#include "../partitioner.hpp"

using namespace  std;

class FennelPartitioner: public Partitioner{
private:
    vid_t num_vertices;
    size_t num_edges;
    size_t filesize;

    int p;
    ifstream fin;

    uint32_t num_batches;
    uint32_t num_edges_per_batch;

//    vector<dense_bitset> node2neis;
//    dense_bitset true_vids;
//    vector<dense_bitset> subg_vids;
//    vector<int> balance_vertex_distribute;
    vector<unordered_set<vid_t>> node2neis;
    unordered_set<vid_t> true_vids;
    vector<unordered_set<vid_t>> subg_vids;
    vector<int> balance_vertex_distribute;

protected:
    void read_and_do(string opt_name);
    void do_fennel();
    void batch_node_assignment(vector<edge_t> &edges);
    void addNeighbors(edge_t &edge);
    int intersection(unordered_set<vid_t>& nums1, unordered_set<vid_t>& nums2);
public:
    FennelPartitioner(string basefilename, string method, int pnum, int memsize, bool shuffle);
    void split();
};


#endif //GRAPHPARTITIONING_FENNEL_HPP
