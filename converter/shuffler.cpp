//
// Created by muzongshen on 2021/9/18.
//

#include <utility>
#include <algorithm>
#include <iostream>
#include <fstream>

#include <stdlib.h>

#include "../util.hpp"
#include "shuffler.hpp"

void Shuffler::init(int memorysize)
{
    num_vertices = 0;
    num_edges = 0;
    nchunks = 0;
    degrees.reserve(1<<30);
    chunk_bufsize = (size_t)memorysize * 1024 * 1024 / sizeof(edge_t);
    LOG(INFO)<<sizeof(edge_t)<<" "<<"memorysize: "<<memorysize<<"chunk_bufsize: "<<chunk_bufsize;
    chunk_buf.reserve(chunk_bufsize);
}

void Shuffler::finalize()
{
    num_vertices=max_vid+1;
    cwrite(edge_t(0, 0), true);
    LOG(INFO) << "num_vertices: " << num_vertices
              << ", num_edges: " << num_edges;
    LOG(INFO) << "number of chunks: " << nchunks;

    std::vector<std::ifstream> fin(nchunks);
    rep (i, nchunks) {
        fin[i].open(chunk_filename(i), std::ios::binary);
        CHECK(fin[i]) << "open chunk " << i << " failed";
    }
    std::vector<bool> finished(nchunks, false);
    std::ofstream fout(shuffled_binedgelist_name(basefilename), std::ios::binary);
    int count = 0;
    fout.write((char *)&num_vertices, sizeof(num_vertices));
    fout.write((char *)&num_edges, sizeof(num_edges));
    while (true) {
        int i = rand() % nchunks;
        if (!fin[i].eof()) {
            edge_t e;
            fin[i].read((char *)&e, sizeof(edge_t));
            if (fin[i].eof()) {
                finished[i] = true;
                count++;
                if (count == nchunks)
                    break;
            } else
                fout.write((char *)&e, sizeof(edge_t));
        }
    }
    rep (i, nchunks)
        fin[i].close();
    fout.close();
    chunk_clean();
    fout.close();

    fout.open(degree_name(basefilename), std::ios::binary);
    fout.write((char *)&degrees[0], num_vertices * sizeof(vid_t));
    fout.close();

    LOG(INFO) << "finished shuffle";
}

void Shuffler::add_edge(vid_t from, vid_t to)
{
    if (to == from) {
        LOG(WARNING) << "Tried to add self-edge " << from << "->" << to
                     << std::endl;
        return;
    }
    if(from>to){
        if(from>max_vid){
            max_vid=from;
        }
    } else{
        if(to>max_vid){
            max_vid=to;
        }
    }

    num_edges++;
//    from = get_vid(from);
//    to = get_vid(to);
    degrees[from]++;
    degrees[to]++;

    edge_t e(from, to);
    cwrite(e);
}

std::string Shuffler::chunk_filename(int chunk)
{
    std::string new_basefilename=change2tmpdir(basefilename);
    std::stringstream ss;
    ss << new_basefilename << "." << chunk << ".chunk";
    LOG(INFO)<<ss.str();
    return ss.str();
}

void Shuffler::chunk_clean()
{
    rep (i, nchunks)
        remove(chunk_filename(i).c_str());
}

void Shuffler::cwrite(edge_t e, bool flush)
{
    if (!flush){
        chunk_buf.push_back(e);
    }
    if (flush || chunk_buf.size() >= chunk_bufsize) {
        work_t work;
        work.shuffler = this;
        work.nchunks = nchunks;
        chunk_buf.swap(work.chunk_buf);
        work();
        nchunks++;
    }
}

