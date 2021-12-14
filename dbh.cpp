//
// Created by muzongshen on 2021/9/23.
//

#include <utility>

#include "partitioner.hpp"
#include "dbh.hpp"
#include "util.hpp"

DbhPartitioner::DbhPartitioner(string basefilename, string method, int pnum, int memsize, bool shuffle)
{
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
    //degree file
    degrees.resize(num_vertices);
    std::ifstream degree_file(degree_name(basefilename), std::ios::binary);
    degree_file.read((char *)&degrees[0], num_vertices * sizeof(vid_t));
    degree_file.close();
    
    num_batches = (filesize/((std::size_t )memsize * 1024 * 1024)) + 1;
    num_edges_per_batch = (num_edges/num_batches) + 1;

    true_vids.resize(num_vertices);
    is_mirrors.assign(num_vertices, dense_bitset(p));
    counter.assign(p, 0);
    part_degrees.assign(num_vertices,vector<vid_t>(p));
    balance_vertex_distribute.resize(num_vertices);
}

void DbhPartitioner::read_and_do(string opt_name){
    fin.seekg(sizeof(num_vertices) + sizeof(num_edges), std::ios::beg);
    std::vector<edge_t> edges;
    auto num_edges_left = num_edges;
    for (uint32_t i = 0; i < num_batches; i++) {
        auto edges_per_batch = num_edges_per_batch < num_edges_left ? num_edges_per_batch : num_edges_left;
        edges.resize(edges_per_batch);
        fin.read((char *) &edges[0], sizeof(edge_t) * edges_per_batch);
        if(opt_name=="dbh"){
            batch_dbh(edges);
        }else if(opt_name=="node_assignment"){
            batch_node_assignment(edges);
        }
        else{
            LOG(ERROR)<<"no valid opt function";
        }
        num_edges_left -= edges_per_batch;
    }
}

void DbhPartitioner::batch_dbh(vector<edge_t> &edges){
    for (auto &e : edges)
    {
        vid_t w = degrees[e.first] <= degrees[e.second] ? e.first : e.second;
        int bucket = w % p;
        counter[bucket]++;
        // save_edge(e->first,e->second,bucket);
        is_mirrors[e.first].set_bit_unsync(bucket);
        is_mirrors[e.second].set_bit_unsync(bucket);
        ++part_degrees[e.first][bucket];
        ++part_degrees[e.second][bucket];
        true_vids.set_bit_unsync(e.first);
        true_vids.set_bit_unsync(e.second);
    }
}

void DbhPartitioner::batch_node_assignment(vector<edge_t> &edges){
    for (auto &e : edges)
    {
        vid_t sp=balance_vertex_distribute[e.first],tp=balance_vertex_distribute[e.second];
        save_edge(e.first,e.second,sp);
        save_edge(e.second,e.first,tp);
    }
}

void DbhPartitioner::split()
{   
    read_and_do("dbh");

    //根据结点平衡性、随机分配的重叠度以及结点的度大小来判断
    size_t total_mirrors = 0;
    vector<vid_t> buckets(p);
    double capacity = (double)true_vids.popcount() * 1.05 / p + 1;
    rep(i, num_vertices){
        total_mirrors += is_mirrors[i].popcount();
        double max_score=0.0;
        vid_t which_p;
        bool unique=false;
        if(is_mirrors[i].popcount()==1){
            unique=true;
        }
        repv(j, p){
            if (is_mirrors[i].get(j)) {
//                double score=((i%p==j)?1:0)+(part_degrees[i][j]/(degrees[i]+1))+(buckets[j]< capacity?1:0);
                double score=(part_degrees[i][j]/(degrees[i]+1))+(buckets[j]< capacity?1:0);
                if (unique){
                    which_p=j;
                }else if (max_score<score){
                    max_score=score;
                    which_p=j;
                }
            }
        }
        ++buckets[which_p];
        save_vertex(i,which_p);
        balance_vertex_distribute[i]=which_p;
    }
    node_fout.close();
    repv(j, p){
         LOG(INFO) << "each partition node count: "<<buckets[j];
    }

    read_and_do("node_assignment");
    total_time.stop();
    edge_fout.close();
    
    // rep(i, p) LOG(INFO) << "edges in partition " << i << ": " << counter[i];
    // LOG(INFO) << "replication factor: " << (double)total_mirrors / true_vids.popcount();
    LOG(INFO) << "total partition time: " << total_time.get_time();
}
