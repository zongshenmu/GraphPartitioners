//
// Created by muzongshen on 2021/9/30.
//

#include "ldg.hpp"
#include <algorithm>
#include <cmath>
#include <algorithm>

LdgPartitioner::LdgPartitioner(string basefilename, string method, int pnum, int memsize, bool shuffle) {
    p=pnum;
    set_write_files(basefilename,method,pnum);

    total_time.start();
    //edge file
    if(shuffle){
        fin.open(shuffled_binedgelist_name(basefilename),std::ios::binary | std::ios::ate);
    }else{
        fin.open(binedgelist_name(basefilename),std::ios::binary | std::ios::ate);
    }
    filesize = fin.tellg();
    fin.seekg(0, std::ios::beg);

    fin.read((char *)&num_vertices, sizeof(num_vertices));
    fin.read((char *)&num_edges, sizeof(num_edges));

    num_batches = (filesize/((std::size_t )memsize * 1024 * 1024)) + 1;
    num_edges_per_batch = (num_edges/num_batches) + 1;

    subg_vids.assign(p,unordered_set<vid_t >{});
//    true_vids.resize(num_vertices);
    balance_vertex_distribute.resize(num_vertices);
    node2neis.assign(num_vertices,unordered_set<vid_t>{});
    LOG(INFO)<<"finish init";
}


void LdgPartitioner::read_and_do(string opt_name) {
    fin.seekg(sizeof(num_vertices) + sizeof(num_edges), std::ios::beg);
    std::vector<edge_t> edges;
    auto num_edges_left = num_edges;
    for (uint32_t i = 0; i < num_batches; i++) {
        auto edges_per_batch = num_edges_per_batch < num_edges_left ? num_edges_per_batch : num_edges_left;
        edges.resize(edges_per_batch);
        fin.read((char *) &edges[0], sizeof(edge_t) * edges_per_batch);
        if(opt_name=="node_assignment"){
            batch_node_assignment(edges);
            LOG(INFO)<<"finish edge";
        }
        else if(opt_name=="process neighbors"){
            for (auto &e : edges){
                addNeighbors(e);
                true_vids.insert(e.first);
                true_vids.insert(e.second);
            }
            LOG(INFO)<<"finish neis";
        }
        else{
            LOG(ERROR)<<"no valid opt function";
        }
        num_edges_left -= edges_per_batch;
    }
}

void LdgPartitioner::addNeighbors(edge_t &edge) {
    node2neis[edge.first].insert(edge.second);
    node2neis[edge.second].insert(edge.first);
}

int LdgPartitioner::intersection(unordered_set<vid_t>& nums1, unordered_set<vid_t>& nums2) {
    // 建立unordered_set存储nums1数组(清除了重复的元素)
    unordered_set<vid_t> ans;
    for(auto num : nums2)
    {
        if(nums1.count(num) == 1)
            ans.insert(num);
    }

    return ans.size();
}

void LdgPartitioner::do_ldg() {
    // Ordering of streaming vertices
    vector<int> ordering(num_vertices);
    for(int i = 0; i < num_vertices; ++i) {
        ordering[i] = i;
    }
    random_shuffle(ordering.begin(), ordering.end());

    // Initial paritions
    for(int i = 0; i < p; ++i) {
        subg_vids[i].insert(ordering[i]);
        save_vertex(ordering[i],i);
    }

    int true_vcount=true_vids.size();
    for (int i=p; i<(vid_t)num_vertices; i++){
        if(i%10000==0)
            cout<<i<<"/"<<num_vertices<<endl;
        vid_t v=ordering[i];
        if (true_vids.find(v)!=true_vids.end()){
//            LOG(INFO)<<"vertex: "<<v;
//            LOG(INFO)<<node2neis[v].size();
            vector<double> from_scores(p, 0);
            for (int id=0; id<p; id++){
                double partitionSize = subg_vids[id].size();
                double weightedGreedy =
                        (1 - (partitionSize / ((double)true_vcount / (double)p)));
                double firstVertextInterCost = intersection(node2neis[v],subg_vids[id]);
                from_scores[id] = firstVertextInterCost * weightedGreedy;
            }
            //最大值所在序列的位置
            int firstIndex = distance(from_scores.begin(),
                    max_element(from_scores.begin(), from_scores.end()));
            balance_vertex_distribute[v]=firstIndex;
            subg_vids[firstIndex].insert(v);
            save_vertex(v,firstIndex);
        }
//        printProgress(i/num_vertices);
    }
    LOG(INFO)<<"finish ldg";
}

void LdgPartitioner::batch_node_assignment(vector<edge_t> &edges) {
    for (auto &e: edges){
        vid_t sp=balance_vertex_distribute[e.first],tp=balance_vertex_distribute[e.second];
        save_edge(e.first,e.second,sp);
        save_edge(e.second,e.first,tp);
    }
}

void LdgPartitioner::split() {
    read_and_do("process neighbors");
    do_ldg();
    read_and_do("node_assignment");
    total_time.stop();
    edge_fout.close();
    LOG(INFO) << "total vertex count: "<< true_vids.size();
    LOG(INFO) << "total partition time: " << total_time.get_time();
}