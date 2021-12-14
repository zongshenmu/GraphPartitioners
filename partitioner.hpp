//
// Created by muzongshen on 2021/9/23.
//

#pragma once

#include <string>
#include <sstream>
#include <iostream>
#include <fstream>

#include "util.hpp"

using namespace std;

class Partitioner
{
protected:
    Timer total_time;
    ofstream edge_fout;
    ofstream node_fout;
public:
    virtual void split() = 0;
    void set_write_files(const string &basefilename,const string &method, int pnum){
        edge_fout.open(partitioned_name(basefilename,method,pnum));
        node_fout.open(nodesave_name(basefilename,method,pnum));
    }

    void save_vertex(vid_t v, int p_id)
    {
        node_fout << v << " "<< p_id <<endl;
    }

    void save_edge(vid_t from, vid_t to, int p_id)
    {
        edge_fout<<from<<" "<<to<<" "<<p_id<<endl;
    }
};
