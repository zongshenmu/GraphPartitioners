//
// Created by muzongshen on 2021/9/24.
//
#include "hdrf.hpp"
#include "util.hpp"

HdrfPartitioner::HdrfPartitioner(string file_name,string method, int pnum, int memsize, double balance_ratio, double balance_lambda, bool shuffle)
{
    set_write_files(file_name,method,pnum);
    num_partitions=pnum;
    lamda=balance_lambda;

    if(shuffle){
        fin.open(shuffled_binedgelist_name(file_name),std::ios::binary | std::ios::ate);
    }else{
        fin.open(binedgelist_name(file_name),std::ios::binary | std::ios::ate);
    }
    filesize = fin.tellg();
    fin.seekg(0, std::ios::beg);

    fin.read((char *)&num_vertices, sizeof(num_vertices));
    fin.read((char *)&num_edges, sizeof(num_edges));
    CHECK_EQ(sizeof(vid_t) + sizeof(size_t) + num_edges * sizeof(edge_t), filesize);
    max_partition_load = balance_ratio * num_edges/num_partitions; // edge load
    degrees.resize(num_vertices, 0);
    num_batches = (filesize/((std::size_t )memsize * 1024 * 1024)) + 1;
    num_edges_per_batch = (num_edges/num_batches) + 1;
    edge_load.resize(pnum);
    vertex_partition_matrix.assign(num_vertices,dense_bitset(pnum));
    true_vids.resize(num_vertices);

    part_degrees.assign(num_vertices,vector<vid_t>(num_partitions));
    balance_vertex_distribute.resize(num_vertices);
}

void HdrfPartitioner::batch_hdrf(vector<edge_t> &edges){
    for (auto &e : edges)
    {
        ++degrees[e.first];
        ++degrees[e.second];

        int max_p = find_max_score_partition_hdrf(e);
        update_vertex_partition_matrix(e, max_p);
        update_min_max_load(max_p);
        // save_edge(e.first,e.second,max_p);
        ++part_degrees[e.first][max_p];
        ++part_degrees[e.second][max_p];
//        save_vertex(e.first,max_p);
//        save_vertex(e.second,max_p);
    }
}

int HdrfPartitioner::find_max_score_partition_hdrf(edge_t& e)
{
    auto degree_u = degrees[e.first];
    auto degree_v = degrees[e.second];

    uint32_t sum;
    double max_score = 0;
    uint32_t max_p = 0;
    double bal, gv, gu;

    for (int p=0; p<num_partitions; p++)
    {
        if (edge_load[p] >= max_partition_load)
        {
            continue;
        }

        gu = 0, gv = 0;
        sum = degree_u + degree_v;
        if (vertex_partition_matrix[e.first].get(p))
        {
            gu = degree_u;
            gu/=sum;
            gu = 1 + (1-gu);
        }
        if (vertex_partition_matrix[e.second].get(p))
        {
            gv = degree_v;
            gv /= sum;
            gv = 1 + (1-gv);
        }

        bal = max_load - edge_load[p];
        if (min_load != UINT64_MAX) bal /= epsilon + max_load - min_load;
        double score_p = gu + gv + lamda*bal;
        if (score_p < 0)
        {
            LOG(ERROR) << "ERROR: score_p < 0";
            LOG(ERROR) << "gu: " << gu;
            LOG(ERROR) << "gv: " << gv;
            LOG(ERROR) << "bal: " << bal;
            exit(-1);
        }
        if (score_p > max_score)
        {
            max_score = score_p;
            max_p = p;
        }
    }
    return max_p;
}

void HdrfPartitioner::update_vertex_partition_matrix(edge_t& e, int max_p)
{
    vertex_partition_matrix[e.first].set_bit_unsync(max_p);
    vertex_partition_matrix[e.second].set_bit_unsync(max_p);
    true_vids.set_bit_unsync(e.first);
    true_vids.set_bit_unsync(e.second);
}

void HdrfPartitioner::update_min_max_load(int max_p)
{
    auto& load = ++edge_load[max_p];
    if (load > max_load) max_load = load;
}

void HdrfPartitioner::batch_node_assign_neighbors(vector<edge_t> &edges){
    for (auto &e : edges)
    {
        vid_t sp=balance_vertex_distribute[e.first],tp=balance_vertex_distribute[e.second];
        save_edge(e.first,e.second,sp);
        save_edge(e.second,e.first,tp);
    }
}

void HdrfPartitioner::read_and_do(string opt_name){
    fin.seekg(sizeof(num_vertices) + sizeof(num_edges), std::ios::beg);
    std::vector<edge_t> edges;
    auto num_edges_left = num_edges;
    for (uint32_t i = 0; i < num_batches; i++) {
        auto edges_per_batch = num_edges_per_batch < num_edges_left ? num_edges_per_batch : num_edges_left;
        edges.resize(edges_per_batch);
        fin.read((char *) &edges[0], sizeof(edge_t) * edges_per_batch);
        if(opt_name=="hdrf"){
            batch_hdrf(edges);
        }else if(opt_name=="node_assignment"){
            batch_node_assign_neighbors(edges);
        }
        else{
            LOG(ERROR)<<"no valid opt function";
        }
        num_edges_left -= edges_per_batch;
    }
}

void HdrfPartitioner::split(){
    Timer total_time;
    total_time.start();

    read_and_do("hdrf");
    
    //根据结点平衡性、随机分配的重叠度以及结点的度大小来判断
    size_t total_mirrors = 0;
    vector<vid_t> buckets(num_partitions);
    double capacity = (double)true_vids.popcount() * 1.05 / num_partitions + 1;
    rep(i, num_vertices){
        total_mirrors += vertex_partition_matrix[i].popcount();
        double max_score=0.0;
        vid_t which_p;
        bool unique=false;
        if(vertex_partition_matrix[i].popcount()==1){
            unique=true;
        }
        repv(j, num_partitions) {
            if (vertex_partition_matrix[i].get(j)) {
//                double score=((i%num_partitions==j)?1:0)+(part_degrees[i][j]/(degrees[i]+1))+(buckets[j]< capacity?1:0);
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
    repv(j, num_partitions){
         LOG(INFO) << "each partition node count: "<<buckets[j];
    }
    
    read_and_do("node_assignment");
    edge_fout.close();

    total_time.stop();
    // rep(i, num_partitions) LOG(INFO) << "edges in partition " << i << ": " << edge_load[i];
    // LOG(INFO) << "replication factor: " << (double)total_mirrors / true_vids.popcount();
    LOG(INFO) << "total partition time: " << total_time.get_time();
}
