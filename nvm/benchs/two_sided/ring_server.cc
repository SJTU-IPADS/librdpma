#include <gflags/gflags.h>
#include <unordered_map>

#include "r2/src/ring_msg/mod.hh"

#include "./r740.hh"

#include "../../huge_region.hh"
#include "../../nvm_region.hh"

#include "../thread.hh"
#include "./constants.hh"
#include "./core.hh"

DEFINE_int64(port, 8888, "Server listener (UDP) port.");
DEFINE_int64(use_nic_idx, 0, "Which NIC to create QP");
DEFINE_int64(reg_mem_name, 73, "The name to register an MR at rctrl.");
DEFINE_int64(threads, 1, "Number of threads used.");

// NVM related settings
DEFINE_bool(use_nvm, true, "Whether to use NVM for RDMA");
DEFINE_bool(non_null, false, "whether to execute null RPC");
DEFINE_string(nvm_file, "/dev/dax1.6", "Abstracted NVM device");
DEFINE_uint64(nvm_sz, 10, "Mapped sz (in GB), should be larger than 2MB");
DEFINE_bool (use_read, true, "read");

DEFINE_bool(clflush, false, "whether to flush write content");

using namespace rdmaio;
using namespace rdmaio::rmem;
using namespace rdmaio::qp;

using namespace r2;
using namespace r2::ring_msg;
using namespace nvm;

template <typename Nat> Nat align(const Nat &x, const Nat &a) {
  auto r = x % a;
  return r ? (x + a - r) : x;
}

template <usize max_reply_entry> class ReplyBufBunder {
  static_assert(max_reply_entry > 0, "");
  std::vector<char *> reply_bufs;
  usize cur = 0;

public:
  explicit ReplyBufBunder(const usize &max_sz, Arc<AbsRecvAllocator> alloc) {
    for (uint i = 0; i < max_reply_entry; ++i)
      reply_bufs.push_back((char *)(std::get<0>(alloc->alloc_one(max_sz).value())));
  }

  char *next_reply_buf() {
    auto res = reply_bufs[cur];
    cur = (cur + 1) % max_reply_entry;
    return res;
  }
};

class SimpleAllocator : public AbsRecvAllocator {
  RMem::raw_ptr_t buf = nullptr;
  usize total_mem = 0;
  mr_key_t key;

  RegAttr mr;

public:
  SimpleAllocator(Arc<RMem> mem, const RegAttr &mr)
      : buf(mem->raw_ptr), total_mem(mem->sz), mr(mr), key(mr.key) {
    // RDMA_LOG(4) << "simple allocator use key: " << key;
  }

  ::r2::Option<std::pair<rmem::RMem::raw_ptr_t, rmem::mr_key_t>>
  alloc_one(const usize &sz) override {
    if (total_mem < sz)
      return {};
    auto ret = buf;
    buf = static_cast<char *>(buf) + sz;
    total_mem -= sz;
    return std::make_pair(ret, key);
  }

  ::rdmaio::Option<std::pair<rmem::RMem::raw_ptr_t, rmem::RegAttr>>
  alloc_one_for_remote(const usize &sz) override {
    if (total_mem < sz)
      return {};
    auto ret = buf;
    buf = static_cast<char *>(buf) + sz;
    total_mem -= sz;
    return std::make_pair(ret, mr);
  }
};

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

  cpu_set_t mask;
  CPU_ZERO(&mask);
  CPU_SET(y, &mask);
  sched_setaffinity(0, sizeof(mask), &mask);

  return 0;
}

int main(int argc, char **argv) {

  gflags::ParseCommandLineFlags(&argc, &argv, true);

  std::vector<gflags::CommandLineFlagInfo> all_flags;
  gflags::GetAllFlags(&all_flags);
  for (auto f : all_flags) {
    if (f.current_value.size())
      RDMA_LOG(2) << f.name << ": " << f.current_value;
  }

  RCtrl ctrl(FLAGS_port, "localhost");
  RingManager<128> rm(ctrl);

  RDMA_LOG(4) << "(RC) NVM server listenes at localhost:" << FLAGS_port
              << " using " << FLAGS_threads << " threads.";

  // init the NVM/DRAM memory
  Arc<MemoryRegion> nvm_region = nullptr;

  if (FLAGS_use_nvm) {
    RDMA_LOG(4) << "server uses NVM with size: " << FLAGS_nvm_sz << " GB";
    u64 sz = static_cast<u64>(FLAGS_nvm_sz) * (1024 * 1024 * 1024L);
    nvm_region = NVMRegion::create(FLAGS_nvm_file, sz).value();
  } else {
    RDMA_LOG(4) << "server uses DRAM (huge page)";
    u64 sz = static_cast<u64>(FLAGS_nvm_sz) * (1024 * 1024 * 1024L);
    // nvm_region = std::make_shared<DRAMRegion>(FLAGS_nvm_sz);
    nvm_region = HugeRegion::create(sz).value();
  }

  // we donot need to register this memory to RNic since this is messaging

  using TThread = Thread<int>;
  std::vector<std::unique_ptr<TThread>> threads;

  for (uint thread_id = 0; thread_id < FLAGS_threads; ++thread_id) {

    using RI = RingRecvIter<ring_entry, ring_sz, max_msg_sz>;

    threads.push_back(std::make_unique<TThread>([thread_id, &ctrl, &rm,
                                                 &nvm_region]() -> int {
                                                  BindToCore(thread_id);
                                                  //bind_to_core(thread_id);
      int idx = 1;
      //int idx = 0;

      while (idx >= RNicInfo::query_dev_names().size() && idx >= 1)
        idx -= 1;
      idx = 1;
      idx = FLAGS_use_nic_idx;

      auto nic = RNic::create(RNicInfo::query_dev_names().at(idx)).value();

      RDMA_ASSERT(ctrl.opened_nics.reg(thread_id, nic));

      //      auto mem = Arc<RMem>(
      // new RMem(64 * 1024 * 1024)); // allocate a memory with 4M bytes
      auto mem_region = HugeRegion::create(128 * 1024 * 1024).value();
      // auto mem_region = DRAMRegion::create(64 * 1024 * 1024).value();
      auto mem = mem_region->convert_to_rmem().value();

      auto handler = RegHandler::create(mem, nic).value();
      auto mr = handler->get_reg_attr().value();

      auto alloc = Arc<SimpleAllocator>(
          new SimpleAllocator(mem, handler->get_reg_attr().value()));

      auto recv_cq_res = ::rdmaio::qp::Impl::create_cq(nic, 2048);
      RDMA_ASSERT(recv_cq_res == IOCode::Ok);
      auto recv_cq = std::get<0>(recv_cq_res.desc);

      auto receiver = RecvFactory<ring_entry, ring_sz, max_msg_sz>::create(
                          rm, std::to_string(thread_id), recv_cq, alloc)
                          .value();

      // init the reply buf of each session
      //std::unordered_map<::r2::ring_msg::id_t, char *> reply_bufs;
      std::unordered_map<::r2::ring_msg::id_t, ReplyBufBunder<ring_entry> *> reply_bufs;

      char *local_buf = new char[4096];

      std::vector<char *> reply_buf_pool;
      const usize max_reply_buf = 256;
      for (uint i = 0;i < max_reply_buf;++i) {
        auto buf = std::get<0>(alloc->alloc_one(4096).value());
        reply_buf_pool.push_back((char *)buf);
      }

      usize cur_idx = 0;
      // now start to recv messages
      while (1) {

        for (RI iter(receiver); iter.has_msgs(); iter.next()) {
          auto msg = iter.cur_msg();
          auto cur_session = iter.cur_session();

          // LOG(4) << "Recv msg sz: " << msg.sz; sleep(1);

          // decode the cur_msg
          MsgHeader *header = msg.interpret_as<MsgHeader>();
          ASSERT(header != nullptr);
          RDMA_ASSERT(header->magic == 73) << "wrong magic #: " << header->magic
                                           << "coro: " << header->coro_id;
          RDMA_ASSERT(header->type == Req);

          // parse the msg
          auto coro_id = header->coro_id;

          // send the reply
          //char reply_buf[4096];
          char *reply_buf = reply_buf_pool[cur_idx];
          cur_idx += 1;
          if (cur_idx >= max_reply_buf) {
            cur_idx = 0;
          }
          //  execute the reply
          usize reply_sz = 0;
          Request *req = msg.interpret_as<Request>(sizeof(MsgHeader));

          if (FLAGS_non_null) {
            if (FLAGS_use_read) {
              ASSERT(req != nullptr);
              memcpy(reply_buf + sizeof(MsgHeader), (char *)nvm_region->addr + req->addr, req->payload);
              reply_sz = req->payload;
            }
            else{
              reply_sz =
                ::nvm::execute_nvm_ops(nvm_region, msg, FLAGS_clflush,reply_buf);
            }
          } else {
            if (FLAGS_use_read) {
              reply_sz = req->payload;
            }
          }
          //auto reply_sz = 0;
          {
            r2::compile_fence();
            MsgHeader *header = (MsgHeader *)(reply_buf);
            header->type = Reply;
            header->sz = reply_sz;
            header->coro_id = coro_id;
          }
#if 1
          auto res_s = cur_session->send_unsignaled(
              {.mem_ptr = (void *)(reply_buf),
               .sz = sizeof(MsgHeader) + reply_sz});
          ASSERT(res_s == IOCode::Ok);
#endif

        }
      }

      return 0;
    }));
  }

  for (auto &t : threads)
    t->start();

  LOG(2) << "all (RC) server threads started";

  sleep(1);

  ctrl.start_daemon();

  for (auto &t : threads) {
    t->join();
  }
  return 0;
}
