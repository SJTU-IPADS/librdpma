#pragma once

#include <numa.h>

// use some utilities (i.e., Option) defined in RLib
#include "rlib/core/common.hh"
#include "rlib/core/rmem/mem.hh"

namespace nvm {

using namespace rdmaio;

struct MemoryRegion {
  u64 sz; // total region size

  void *addr = nullptr; // region

  void *start_ptr() const { return addr; }

  u64 size() const { return sz; }

  MemoryRegion() = default;

  MemoryRegion(const u64 &sz, void *addr) : sz(sz), addr(addr) {}

  virtual bool valid() { return addr != nullptr; }

  ::rdmaio::Option<Arc<rmem::RMem>> convert_to_rmem() {
    if (!valid())
      return {};
    return std::make_shared<rmem::RMem>(sz, [this](u64 s) { return addr; },
                                        [](rmem::RMem::raw_ptr_t p) {});
  }
};

class DRAMRegion : public MemoryRegion {
public:
  explicit DRAMRegion(const u64 &sz) : MemoryRegion(sz, malloc(sz)) {
    RDMA_ASSERT(this->addr != nullptr);
  }

  DRAMRegion(const u64 &sz, const int &numa_node) : MemoryRegion(sz, numa_alloc_onnode(sz, numa_node)) {
    RDMA_ASSERT(this->addr != nullptr);

  }

  static ::rdmaio::Option<Arc<DRAMRegion>> create(const u64 &sz) {
    return std::make_shared<DRAMRegion>(sz);
  }

  static ::rdmaio::Option<Arc<DRAMRegion>> create(const u64 &sz,const int &numa) {
    return std::make_shared<DRAMRegion>(sz,numa);
  }
};
} // namespace nvm
