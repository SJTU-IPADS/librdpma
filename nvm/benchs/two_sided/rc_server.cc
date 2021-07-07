#include <gflags/gflags.h>

#include <unordered_map>

#include "rlib/core/lib.hh"
#include "rlib/core/qps/rc_recv_manager.hh"
#include "rlib/core/qps/recv_iter.hh"

#include "r2/src/mem_block.hh"
#include "r2/src/msg/rc_session.hh"

#include "./proto.hh"
#include "./r740.hh"

#include "../../huge_region.hh"
#include "../../nvm_region.hh"

#include "../thread.hh"

DEFINE_int64(port, 8888, "Server listener (UDP) port.");
DEFINE_int64(use_nic_idx, 0, "Which NIC to create QP");
DEFINE_int64(reg_mem_name, 73, "The name to register an MR at rctrl.");
DEFINE_int64(threads, 1, "Number of threads used.");

// NVM related settings
DEFINE_bool(use_nvm, true, "Whether to use NVM for RDMA");
DEFINE_string(nvm_file, "/dev/dax1.0", "Abstracted NVM device");
DEFINE_uint64(nvm_sz, 2L * 1024 * 1024 * 1024,
              "Mapped sz, should be larger than 2MB");

DEFINE_bool(clfush, false, "whether to flush write content");

using namespace rdmaio;
using namespace rdmaio::rmem;
using namespace rdmaio::qp;

using namespace r2;
using namespace nvm;

void flush_cache(char *ptr, u64 size) {
  for (u64 cur = 0; cur < size; cur += 64) {
    _mm_clflush(ptr);
    ptr += 64;
  }
}

template <typename Nat> Nat align(const Nat &x, const Nat &a) {
  auto r = x % a;
  return r ? (x + a - r) : x;
}

// an allocator to allocate recv buffer for messages
// AbsRecvAllocator: defined in rlib/core/qps/recv_helper.hh
class SimpleAllocator : public AbsRecvAllocator {
  RMem::raw_ptr_t buf = nullptr;
  usize total_mem = 0;
  mr_key_t key;

public:
  SimpleAllocator(Arc<RMem> mem, mr_key_t key)
      : buf(mem->raw_ptr), total_mem(mem->sz), key(key) {
    // RDMA_LOG(4) << "simple allocator use key: " << key;
  }

  ::r2::Option<std::pair<rmem::RMem::raw_ptr_t, rmem::mr_key_t>>
  alloc_one(const usize &sz) override {
    auto asz = align(sz, 8192U);
    if (total_mem < asz)
      return {};
    auto ret = buf;
    buf = static_cast<char *>(buf) + asz;
    total_mem -= asz;
    return std::make_pair(ret, key);
  }

  ::rdmaio::Option<std::pair<rmem::RMem::raw_ptr_t, rmem::RegAttr>>
  alloc_one_for_remote(const usize &sz) override {
    return {};
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

  RCtrl ctrl(FLAGS_port);
  RecvManager<128, 2048> manager(ctrl);

  RDMA_LOG(4) << "(RC) NVM server listenes at localhost:" << FLAGS_port
              << " using " << FLAGS_threads << " threads.";

  // init the NVM/DRAM memory
  Arc<MemoryRegion> nvm_region = nullptr;

  if (FLAGS_use_nvm) {
    RDMA_LOG(4) << "server uses NVM with size: " << FLAGS_nvm_sz;
    nvm_region = NVMRegion::create(FLAGS_nvm_file, FLAGS_nvm_sz).value();
  } else {
    RDMA_LOG(4) << "server uses DRAM (huge page)";
    // nvm_region = std::make_shared<DRAMRegion>(FLAGS_nvm_sz);
    nvm_region = HugeRegion::create(FLAGS_nvm_sz).value();
  }

  // we donot need to register this memory to RNic since this is messaging

  using TThread = Thread<int>;
  std::vector<std::unique_ptr<TThread>> threads;

  for (uint thread_id = 0; thread_id < FLAGS_threads; ++thread_id) {

    threads.push_back(std::make_unique<TThread>([thread_id, &ctrl, &manager,
                                                 nvm_region]() -> int {
                                                  //      BindToCore(thread_id);
      int idx = 0;
      auto nic = RNic::create(RNicInfo::query_dev_names().at(idx)).value();

      RDMA_ASSERT(ctrl.opened_nics.reg(thread_id, nic));

      // 1. create recv commm data structure
      auto recv_cq_res = ::rdmaio::qp::Impl::create_cq(nic, 4096);
      RDMA_ASSERT(recv_cq_res == IOCode::Ok);
      auto recv_cq = std::get<0>(recv_cq_res.desc);

      // TODO: impl
      // alloc memory for recv buffers
      //auto mem = Arc<RMem>(
      //new RMem(64 * 1024 * 1024)); // allocate a memory with 4M bytes

      auto mem_region = HugeRegion::create(64 * 1024 * 1024).value();
      auto mem = mem_region->convert_to_rmem().value();

      auto handler = RegHandler::create(mem, nic).value();
      auto mr = handler->get_reg_attr().value();
      ctrl.registered_mrs.reg(thread_id, handler);

      Arc<AbsRecvAllocator> alloc = std::make_shared<SimpleAllocator>(
          mem, handler->get_reg_attr().value().key);

      manager.reg_recv_cqs.create_then_reg(std::to_string(thread_id), recv_cq,
                                           alloc);

      // this is benchmark code, so there is memory leakage anyway
      std::unordered_map<u32, RCRecvSession<128> *> incoming_sessions;

      usize counter = 0;
      while (1) {
        ibv_wc wcs[4096];

        for (RecvIter<RC, 4096> iter(recv_cq, wcs); iter.has_msgs();
             iter.next()) {
          auto imm_msg = iter.cur_msg().value();
          auto buf = static_cast<char *>(std::get<1>(imm_msg));
          auto session_id = std::get<0>(imm_msg);

          //RDMA_LOG(4) << "receive one msg: " << (void *)buf;

          MemBlock msg(buf, 4096); // 4096 means each message's size
          MsgHeader *header = msg.interpret_as<MsgHeader>();

          // sanity check header content
          RDMA_ASSERT(header->magic == 73) << "wrong magic #: " << header->magic
                                           << "coro: " << header->coro_id << "; session_id: " << session_id;
          //if (header->magic != 73) continue;

          auto coro_id = header->coro_id;

          RCRecvSession<128> *endpoint = nullptr;

          switch (header->type) {
          case Connect: {
            if (incoming_sessions.find(session_id) == incoming_sessions.end()) {
              // insert the current session
              auto s_qp = std::dynamic_pointer_cast<RC>(
                  ctrl.registered_qps.query(std::to_string(session_id))
                      .value());
              auto s_rs =
                  manager.reg_recv_entries.query(std::to_string(session_id))
                      .value();

              ConnectReq2 *req =
                  msg.interpret_as<ConnectReq2>(sizeof(MsgHeader));
              s_qp->bind_remote_mr(req->attr);
              s_qp->bind_local_mr(mr);

              auto rs = new RCRecvSession<128>(s_qp, s_rs);
              incoming_sessions.insert(std::make_pair(session_id, rs));
            } else
              ASSERT(false);

            // send back the reply
            MemBlock reply(buf, sizeof(MsgHeader) + 1024);
            // MemBlock reply(buf, sizeof(MsgHeader) + 1024);
            {
              MsgHeader *header = (MsgHeader *)buf;
              header->type = Reply;
              header->sz = 73;
              header->coro_id = coro_id;
            }

            // send back
            endpoint = (incoming_sessions[session_id]);
            ASSERT(endpoint->end_point.send_unsignaled(reply) == IOCode::Ok);
          } break;
          case Req: {
            char reply_buf[64];
            MemBlock reply(reply_buf, 64);
            // MemBlock reply(buf, 64);
            {
              r2::compile_fence();
              MsgHeader *header = (MsgHeader *)(reply.mem_ptr);
              header->type = Reply;
              header->sz = 0;
              header->coro_id = coro_id;
            }

            r2::compile_fence();
            endpoint = (incoming_sessions[session_id]);
            //RDMA_LOG(4) << "reply req to qp: "
                        //<< endpoint->end_point.qp << "; check recv cq: " << recv_cq<< "\nQP's two cq: "
            //<< endpoint->end_point.qp->cq << " " <<
            //endpoint->end_point.qp->recv_cq;
            ASSERT(endpoint->end_point.send_unsignaled(reply) == IOCode::Ok);
          } break;
          default:
            ASSERT(false) << "receive a req of: " << (int)header->type;
          }

          ASSERT(endpoint != nullptr);
          endpoint->consume_one();

          // end receiving messages
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
