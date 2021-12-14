//
// Created by muzongshen on 2021/9/18.
//

#pragma once

#include <utility>
#include <chrono>
#include <stdint.h>
#include <sys/stat.h>
#include <glog/logging.h>

#define rep(i, n) for (int i = 0; i < (int)(n); ++i)
#define repv(i, n) for (vid_t i = 0; i < n; ++i)

typedef uint32_t vid_t;
const vid_t INVALID_VID = -1;
struct edge_t {
    vid_t first, second;
    edge_t() : first(0), second(0) {}
    edge_t(vid_t first, vid_t second) : first(first), second(second) {}
    const bool valid() { return first != INVALID_VID; }
    void remove() { first = INVALID_VID; }
};

inline std::string change2tmpdir(const std::string& str)
{
    std::string dir_path,file_name;
    std::size_t found = str.find_last_of("/");
    dir_path=str.substr(0,found);
    file_name=str.substr(found+1);
    dir_path+="/tmp_partitioning_dir";
    std::string cmd="mkdir -p "+dir_path;
    int flag=system(cmd.c_str());
    if(flag==1){
        LOG(FATAL)<<"make tmp dir error!";
    }
    return dir_path+"/"+file_name;
}

void writea(int f, char *buf, size_t nbytes);

inline std::string binedgelist_name(const std::string &basefilename)
{
    std::string new_basefilename=change2tmpdir(basefilename);
    std::stringstream ss;
    ss << new_basefilename << ".binedgelist";
    return ss.str();
}
inline std::string shuffled_binedgelist_name(const std::string &basefilename)
{
    std::string new_basefilename=change2tmpdir(basefilename);
    std::stringstream ss;
    ss << new_basefilename << ".shuffled.binedgelist";
    return ss.str();
}
inline std::string degree_name(const std::string &basefilename)
{
    std::string new_basefilename=change2tmpdir(basefilename);
    std::stringstream ss;
    ss << new_basefilename << ".degree";
    return ss.str();
}
inline std::string partitioned_name(const std::string &basefilename,const std::string &method, int pnum)
{
    std::string new_basefilename=change2tmpdir(basefilename);
    std::stringstream ss;
    ss << new_basefilename << "."<<method<<".edgepart." << pnum; // chenzi: add partition number to the output file
    return ss.str();
}
inline std::string nodesave_name(const std::string &basefilename,const std::string &method, int pnum)
{
    std::string new_basefilename=change2tmpdir(basefilename);
    std::stringstream ss;
    ss << new_basefilename << "."<<method<<".nodes." << pnum; // chenzi: add partition number to the output file
    return ss.str();
}
inline bool is_exists(const std::string &name)
{
    struct stat buffer;
    return (stat(name.c_str(), &buffer) == 0);
}

class Timer
{
private:
    std::chrono::system_clock::time_point t1, t2;
    double total;

public:
    Timer() : total(0) {}
    void reset() { total = 0; }
    void start() { t1 = std::chrono::system_clock::now(); }
    void stop()
    {
        t2 = std::chrono::system_clock::now();
        std::chrono::duration<double> diff = t2 - t1;
        total += diff.count();
    }
    double get_time() { return total; }
};
