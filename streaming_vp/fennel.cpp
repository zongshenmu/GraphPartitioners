//
// Created by muzongshen on 2021/9/30.
//

#include "fennel.hpp"
#include <algorithm>
#include <cmath>

FennelPartitioner::FennelPartitioner(string basefilename, string method, int pnum, int memsize, bool shuffle) {
    p=pnum;
    set_write_files(basefilename,method,pnum);
    LOG(INFO)<<"begin init class";
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

//    subg_vids.assign(p,dense_bitset(num_vertices));
//    true_vids.resize(num_vertices);
//    balance_vertex_distribute.resize(num_vertices);
//    node2neis.assign(num_vertices,dense_bitset(num_vertices));

    //unorderedset
    subg_vids.assign(p,unordered_set<vid_t >{});
//    true_vids.resize(num_vertices);
    balance_vertex_distribute.resize(num_vertices);
    node2neis.assign(num_vertices,unordered_set<vid_t>{});
    LOG(INFO)<<"finish init";
}


void FennelPartitioner::read_and_do(string opt_name) {
    fin.seekg(sizeof(num_vertices) + sizeof(num_edges), std::ios::beg);
    std::vector<edge_t> edges;
    auto num_edges_left = num_edges;
    for (uint32_t i = 0; i < num_batches; i++) {
        auto edges_per_batch = num_edges_per_batch < num_edges_left ? num_edges_per_batch : num_edges_left;
        edges.resize(edges_per_batch);
        fin.read((char *) &edges[0], sizeof(edge_t) * edges_per_batch);
        if(opt_name=="node_assignment"){
            batch_node_assignment(edges);
        }
        else if(opt_name=="process neighbors"){
            int i=0;
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

void FennelPartitioner::addNeighbors(edge_t &edge) {
    node2neis[edge.first].insert(edge.second);
    node2neis[edge.second].insert(edge.first);
}

int FennelPartitioner::intersection(unordered_set<vid_t>& nums1, unordered_set<vid_t>& nums2) {
    // 建立unordered_set存储nums1数组(清除了重复的元素)
    unordered_set<vid_t> ans;
    for(auto num : nums2)
    {
        if(nums1.count(num) == 1)
            ans.insert(num);
    }
    return ans.size();
}

void FennelPartitioner::do_fennel() {
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
    const double gamma = 3 / 2.0;
    const double alpha =
            num_edges * pow(p, (gamma - 1)) / pow(true_vcount, gamma);
    const double load_limit = 1.1 * true_vcount /p;
    for (int i=0; i<(vid_t)num_vertices; i++){
        vid_t v=ordering[i];
        if(i%10000==0)
            cout<<i<<"/"<<num_vertices<<endl;
        if (true_vids.find(v)!=true_vids.end()){
            vector<double> from_scores(p, 0);
            for (int id=0; id<p; id++){
                double partitionSize = subg_vids[id].size();
                if (partitionSize<=load_limit){
                    double firstVertextIntraCost;
                    double weightedGreedy =
                            (1 - (partitionSize / ((double)true_vcount / (double)p)));
                    double firstVertextInterCost = intersection(node2neis[v], subg_vids[id]);
                    firstVertextIntraCost = alpha * gamma * pow(partitionSize, gamma-1);
                    from_scores[id] = firstVertextInterCost - firstVertextIntraCost;
                }
            }
            //最大值所在序列的位置
            int firstIndex = distance(from_scores.begin(),
                                      max_element(from_scores.begin(), from_scores.end()));
            balance_vertex_distribute[v]=firstIndex;
            subg_vids[firstIndex].insert(v);
            save_vertex(v,firstIndex);
        }
    }
}

void FennelPartitioner::batch_node_assignment(vector<edge_t> &edges) {
    for (auto &e: edges){
        vid_t sp=balance_vertex_distribute[e.first],tp=balance_vertex_distribute[e.second];
        save_edge(e.first,e.second,sp);
        save_edge(e.second,e.first,tp);
    }
}

void FennelPartitioner::split() {
    LOG(INFO)<<"begin process neighbors";
    read_and_do("process neighbors");
    LOG(INFO)<<"begin write nodes";
    do_fennel();
    LOG(INFO)<<"begin write edges";
    read_and_do("node_assignment");
    total_time.stop();
    edge_fout.close();
    LOG(INFO) << "total vertex count: "<< true_vids.size();
    LOG(INFO) << "total partition time: " << total_time.get_time();
}