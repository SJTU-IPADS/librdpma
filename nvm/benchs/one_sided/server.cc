#include <gflags/gflags.h>

#include <numa.h>

#include "../../huge_region.hh"
#include "../../nvm_region.hh"
#include "rlib/core/lib.hh"

#include "../thread.hh"
#include "../two_sided/r740.hh"

using namespace nvm;

DEFINE_int32(numa_node,0, "Server numa node used.");

DEFINE_int64(port, 8888, "Server listener (UDP) port.");
DEFINE_string(host, "192.168.13.155",
              "NVM server host (we only have one NVM machine :)");

DEFINE_int64(use_nic_idx, 0, "Which NIC to create QP");
DEFINE_int64(reg_nic_name, 73, "The name to register an opened NIC at rctrl.");
DEFINE_int64(reg_mem_name, 73, "The name to register an MR at rctrl.");
DEFINE_uint64(magic_num, 0xdeadbeaf, "The magic number read by the client");

DEFINE_bool(use_nvm, true, "Whether to use NVM for RDMA");
DEFINE_string(nvm_file, "/dev/dax1.0", "Abstracted NVM device");
DEFINE_uint64(nvm_sz, 2L, "Mapped sz (in GB), should be larger than 2MB");

DEFINE_bool(touch_mem, false,
            "whether to warm the LLC of the benchmark memory");

using namespace rdmaio;
using namespace rdmaio::rmem;

int BindToCore(int t_id) {

  if (t_id >= (per_socket_cores * 2))
    return 0;

  int x = t_id;
  int y = 0;

#ifdef SCALE
  assert(false);
  // specific  binding for scale tests
  int mac_per_node = 16 / nthreads; // there are total 16 threads avialable
  int mac_num = current_partition % mac_per_node;

  if (mac_num < mac_per_node / 2) {
    y = socket_0[x + mac_num * nthreads];
  } else {
    y = socket_one[x + (mac_num - mac_per_node / 2) * nthreads];
  }
#else
  // bind ,andway
  if (x >= per_socket_cores) {
    // there is no other cores in the first socket
    y = socket_one[x - per_socket_cores];
  } else {
    y = socket_zero[x];
  }

#endif

  // fprintf(stdout,"worker: %d binding %d\n",x,y);
  cpu_set_t mask;
  CPU_ZERO(&mask);
  CPU_SET(y, &mask);
  sched_setaffinity(0, sizeof(mask), &mask);

  return 0;
}

int main(int argc, char **argv) {

  gflags::ParseCommandLineFlags(&argc, &argv, true);

  RDMA_LOG(4) << "total: "
         << numa_max_node() << " numa nodes";

  std::vector<gflags::CommandLineFlagInfo>
    all_flags;
  gflags::GetAllFlags(&all_flags);
  for (auto f : all_flags) {
    if (f.current_value.size())
      RDMA_LOG(2) << f.name << ": " << f.current_value;
  }

  RCtrl ctrl(FLAGS_port, FLAGS_host);
  RDMA_LOG(4) << "*NVM server* listenes at " << FLAGS_host << ":" << FLAGS_port;

  Arc<MemoryRegion> nvm_region = nullptr;
  Arc<MemoryRegion> dram_region =
      HugeRegion::create(1024 * 1024 * 1024L * 2).value();

  Arc<MemoryRegion> dram_region1 =
      HugeRegion::create(1024 * 1024 * 1024L * 2).value();

  // first we open the NIC
  {
    for (uint i = 0; i < RNicInfo::query_dev_names().size(); ++i) {
      auto nic = RNic::create(RNicInfo::query_dev_names().at(i)).value();
      //if (i == 0) {
      RDMA_LOG(4) << "querid device: "
             << RNicInfo::query_dev_names().at(i);
                // register the nic with name 0
                RDMA_ASSERT(ctrl.opened_nics.reg(i, nic));
      //}
    }
  }

  RDMA_LOG(2) << "opened all nics done";

  {
    Arc<RMem> rmem = nullptr;
    u64 sz = static_cast<u64>(FLAGS_nvm_sz) * (1024 * 1024 * 1024L);

    {
      if (FLAGS_use_nvm) {
        RDMA_LOG(4) << "server uses NVM!";
        nvm_region = NVMRegion::create(FLAGS_nvm_file, sz).value();
      } else {
        RDMA_LOG(4) << "server uses DRAM";
        //nvm_region = DRAMRegion::create(sz,FLAGS_numa_node).value();
        nvm_region = HugeRegion::create(sz).value();
      }
    }

    RDMA_LOG(2) << "init memory region done";

    rmem = std::make_shared<RMem>( // nvm_region->size(),
        sz,
        [&nvm_region](u64 s) -> RMem::raw_ptr_t {
          return nvm_region->start_ptr();
        },
        [](RMem::raw_ptr_t ptr) {
          // currently we donot free the resource, since the region will use
          // to the end
        });
    for (uint i = 0; i < RNicInfo::query_dev_names().size(); ++i) {
      RDMA_ASSERT(ctrl.registered_mrs.create_then_reg(
                                                      i, rmem, ctrl.opened_nics.query(i).value())) << " reg MR for nic: " << i << " failed";
      RDMA_ASSERT(ctrl.registered_mrs.create_then_reg(i * 73 + 73, dram_region->convert_to_rmem().value(),
                                                      ctrl.opened_nics.query(i).value()));
    }
  }
  RDMA_LOG(2) << "reg all memory done";

#if 0
  // initialzie the value
  u64 *reg_mem = (u64 *)(ctrl.registered_mrs.query(FLAGS_reg_mem_name)
                             .value()
                             ->get_reg_attr()
                             .value()
                             .buf);
#endif

  if (FLAGS_touch_mem) {
    // we touch the memory to fill cache line
    // thread on NUMA 0
    Thread<int> t0([&nvm_region]() -> int {
      BindToCore(0);
      auto start_ptr = nvm_region->addr;
      memset(start_ptr, 0, 14080 * 1024);
      return 0;
    });
    t0.start();
    t0.join();

    // thread on NUMA 1
    Thread<int> t1([&nvm_region]() -> int {
      BindToCore(per_socket_cores + 1);
      auto start_ptr = nvm_region->addr;
      memset(start_ptr, 0, 14080 * 1024);
      return 0;
    });
    t1.start();
    t1.join();
  }
  ctrl.start_daemon();

  RDMA_LOG(2) << "RC nvm server started!";
  while (1) {
    // server does nothing because it is RDMA
    sleep(1);
  }
}
