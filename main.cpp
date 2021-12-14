//pnum "number of parititions"
//memsize "memory size in MB. Memsize is used as a chunk size if shuffling and batch size while partitioning"
//method "partition method"
//dataset "name of the dataset to construct graph."
//filename "name of the file to store edge list of a graph."
//shuffle "for streaming graph, influncing the partitioning quality"

#include <iostream>
#include "converter/conversions.hpp"
#include "converter/shuffler.hpp"
#include "util.hpp"
#include "ne_partitioner/ne.hpp"
#include "dbh.hpp"
#include "hdrf.hpp"
#include "streaming_vp/ldg.hpp"
#include "streaming_vp/fennel.hpp"

using namespace std;

int main() {
    int pnum=150;
    int memsize=4096;
    string method="fennel";
    double lambda=1.1;
    double balance_ratio=1.05;
    string edgename="/data0/mzs/Code/MPGraph/data/products/walks.txt";

    bool shuffle=false;

    google::InitGoogleLogging("main");  //参数为自己的可执行文件名
    FLAGS_logtostderr = true;

    // read the edges and change to binary format file
    Converter* converter;
    string binedgelist; // binary edgelist file
    LOG(INFO) << "Using normal dataset";
    converter = new Converter(edgename);
//    LOG(INFO) << "Using shuffled dataset";
//    converter = new Shuffler(edgename);
    convert(edgename, converter, memsize);

    Partitioner *partitioner = NULL;
    if (method=="ne")
        partitioner = new NePartitioner(edgename, method, pnum);
    else if (method=="dbh")
        partitioner = new DbhPartitioner(edgename, method, pnum, memsize, shuffle);
    else if (method=="hdrf")
        partitioner = new HdrfPartitioner(edgename,method,pnum,memsize,balance_ratio,lambda,shuffle);
    else if (method=="ldg")
        partitioner = new LdgPartitioner(edgename, method, pnum, memsize, shuffle);
    else if (method=="fennel")
        partitioner = new FennelPartitioner(edgename, method, pnum, memsize, shuffle);
    else
        LOG(ERROR) << "unkown method: " << method;
    partitioner->split();

    google::ShutdownGoogleLogging();
    return 0;
}
