#include "blimit.hpp"

unsigned int bvalue(unsigned int method, unsigned long node_id) {
    return (node_id * (method + 1)) % 20;
}
