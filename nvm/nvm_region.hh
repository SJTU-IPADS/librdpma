#pragma once

#include <fcntl.h>
#include <fstream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <unistd.h>
#include <xmmintrin.h>

#include "./memory_region.hh"

namespace nvm {

class NVMRegion : public MemoryRegion {

  int fd;

  /*!
    create an nvm region in "fsdax" mode
   */
  explicit NVMRegion(const std::string &nvm_file)
      : fd(open(nvm_file.c_str(), O_RDWR)) {
    if (fd >= 0) {
      this->sz =
          (long long int)(std::ifstream(nvm_file, std::ifstream::ate |
                                                      std::ifstream::binary)
                              .tellg());
      this->addr = mmap(nullptr, sz, PROT_WRITE | PROT_READ,
                        MAP_SHARED | MAP_HUGETLB, fd, 0);
      RDMA_ASSERT(false);
    }

    if (!this->valid()) {
      RDMA_LOG(4) << "not vaild: " << strerror(errno);
    }
  }

  /*!
    create an nvm region in "devdax" mode
    the region in this create can register with RDMA, hopely
   */
  NVMRegion(const std::string &nvm_dev, u64 sz)
      : fd(open(nvm_dev.c_str(), O_RDWR)) {

    //auto map_flag =
    //      MAP_SHARED | MAP_ANONYMOUS | MAP_POPULATE | MAP_HUGETLB;
    //auto map_flag = MAP_SHARED | MAP_HUGETLB | MAP_POPULATE;
    auto map_flag = MAP_SHARED;
    this->addr =
      mmap(nullptr, sz, PROT_WRITE | PROT_READ, map_flag,
           fd, 0);
    this->sz = sz;
    if (!this->valid())
      RDMA_LOG(4)

          << "mapped fd: " << fd << "; addr: " << addr
          << ", check error:" << strerror(errno);
    //RDMA_ASSERT(false);
  }

  bool valid() override { return fd >= 0 && addr != nullptr; }

public:
  static Option<Arc<NVMRegion>> create(const std::string &nvm_file) {
    auto region = Arc<NVMRegion>(new NVMRegion(nvm_file));
    if (region->valid())
      return region;
    return {};
  }

  static Option<Arc<NVMRegion>> create(const std::string &nvm_file, u64 sz) {
    auto region = Arc<NVMRegion>(new NVMRegion(nvm_file, sz));
    if (region->valid())
      return region;
    return {};
  }

  ~NVMRegion() {
    if (valid()) {
      close(fd);
    }
  }

}; // namespace nvm

} // namespace nvm
