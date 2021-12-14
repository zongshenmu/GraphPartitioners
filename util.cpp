//
// Created by muzongshen on 2021/9/18.
//

#include <iterator>
#include <algorithm>
#include <functional>

#include "util.hpp"

void writea(int f, char *buf, size_t nbytes)
{
    size_t nwritten = 0;
    while (nwritten < nbytes) {
        ssize_t a = write(f, buf, nbytes - nwritten);
        PCHECK(a != ssize_t(-1)) << "Could not write " << (nbytes - nwritten)
                                 << " bytes!";
        buf += a;
        nwritten += a;
    }
}