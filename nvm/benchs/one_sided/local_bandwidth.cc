#include <gflags/gflags.h>

#include "../../nvm_region.hh"

using namespace nvm;

#include "../two_sided/core.hh"

DEFINE_uint32(payload, 2, "The sequential write payload ()");

int main(int argc, char **argv) {

  gflags::ParseCommandLineFlags(&argc, &argv, true);

  r2::Timer timer;
  u64 write_transfered = 0;
  u64 write_cnt = 0;

  char *local_mem = new char[FLAGS_payload];

  u32 off = 0;
  while (true) {
    // perform write
    // 4096 ensures that in the same DIMM
    write_transfered += nt_memcpy(local_mem, FLAGS_payload, dst + off % 4096);
  }
}
