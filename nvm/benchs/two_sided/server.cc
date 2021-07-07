#include <gflags/gflags.h>

#include <unordered_map>

#include "rlib/core/lib.hh"
#include "rlib/core/qps/recv_iter.hh"

#include "r2/src/mem_block.hh"
#include "r2/src/msg/ud_session.hh"

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
class SimpleAllocator : AbsRecvAllocator {
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
  RDMA_LOG(4) << "(UD) NVM server listenes at localhost:" << FLAGS_port
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

    threads.push_back(std::make_unique<TThread>([thread_id, &ctrl,
                                                 nvm_region]() -> int {
    // BindToCore(thread_id + per_socket_cores);
                                                  BindToCore(thread_id);
                                                  //bind_to_core(thread_id);
      auto idx = FLAGS_use_nic_idx;

      auto nic = RNic::create(RNicInfo::query_dev_names().at(idx)).value();
      // prepare the message buf

      auto mem_region = std::make_shared<DRAMRegion>(16 * 1024 * 1024);
      // auto mem_region = HugeRegion::create(64 * 1024 * 1024).value();
      auto mem = mem_region->convert_to_rmem().value();

      auto handler = RegHandler::create(mem, nic).value();
      SimpleAllocator alloc(mem, handler->get_reg_attr().value().key);

      // prepare buffer, contain 2048 recv entries, each has 4096 bytes
      auto recv_rs =
          RecvEntriesFactory<SimpleAllocator, 2048, 4096>::create(alloc);
      // ctrl.registered_mrs.reg(FLAGS_reg_mem_name, handler);

      auto ud = UD::create(nic, QPConfig().set_qkey(thread_id + 73)).value();
      ctrl.registered_qps.reg(std::to_string(thread_id), ud);

      // post these recvs to the UD
      {
        recv_rs->sanity_check();
        auto res = ud->post_recvs(*recv_rs, 2048);
        RDMA_ASSERT(res == IOCode::Ok);
      }

      auto mr = handler->get_reg_attr().value();

      std::unordered_map<u32, Arc<UDSession>> incoming_sessions;

      r2::Timer t;

      Arc<UDSession> reply_s = nullptr;
      DoorbellHelper<16> doorbell(IBV_WR_SEND_WITH_IMM);

      u64 counter = 0;

      // while (t.passed_sec() < 100) {

      while (1) {
        u64 sum = 0;
        for (RecvIter<UD, 2048> iter(ud, recv_rs); iter.has_msgs();
             iter.next()) {
          auto imm_msg = iter.cur_msg().value();

          auto session_id = std::get<0>(imm_msg);
          auto buf = static_cast<char *>(std::get<1>(imm_msg)) + kGRHSz;

          // wrapper the message to avoid overflow
          MemBlock msg(buf, 4096 - kGRHSz);
          MsgHeader *header = msg.interpret_as<MsgHeader>();
          RDMA_ASSERT(header != nullptr);

          // sanity check header content
          RDMA_ASSERT(header->magic == 73);

          switch (header->type) {
          case Connect: {
            // RDMA_LOG(2) << "Recv incoming connect from session: " <<
            // session_id;

            ConnectReq *req = msg.interpret_as<ConnectReq>(sizeof(MsgHeader));
            if (incoming_sessions.find(session_id) == incoming_sessions.end()) {
              incoming_sessions.insert(std::make_pair(
                  session_id,
                  UDSession::create(session_id, ud, req->attr).value()));
              if (reply_s == nullptr)
                reply_s = incoming_sessions[session_id];
            } else {
              assert(false);
              incoming_sessions[session_id] =
                  UDSession::create(session_id, ud, req->attr).value();
            }

            { //
              ConnectReply *reply =
                  msg.interpret_as<ConnectReply>(sizeof(MsgHeader));
              reply->session_id = session_id;
              reply->coro_id = header->coro_id;
            }

            {
              header->type = Reply;
              header->sz = sizeof(ConnectReply);
            }
            // assert(incoming_sessions.find(session_id) !=
            // incoming_sessions.end());
            RDMA_ASSERT(incoming_sessions[session_id]->send_unsignaled(
                            msg, mr.key) == IOCode::Ok);

          } break;
          case Req: {
            counter += 1;
            // this is the main body for handling the requests
            /*
              1. Handling requests
             */
            u32 reply_sz = 0;
            auto coro_id = header->coro_id;
            {
              Request *req = msg.interpret_as<Request>(sizeof(MsgHeader));

              switch (req->read) {
              case 0:
                // write case
                {
                  char *payload = msg.interpret_as<char>(sizeof(MsgHeader) +
                                                         sizeof(Request));
                  char *server_buf_ptr =
                      reinterpret_cast<char *>(nvm_region->addr) + req->addr;
                  r2::compile_fence();
                  sum += (u64)(memcpy(server_buf_ptr, payload, req->payload));
                  r2::compile_fence();
                  flush_cache(server_buf_ptr, req->payload);
                  r2::compile_fence();
                }
                break;
              case 1: {
                /* read case
                   this microbenchmark copy the server_buf to reply_buf (in
                   message) the addr and size is specificed in Request
                */
#if 0
                char *reply_buf = buf + sizeof(MsgHeader);

                char *server_buf_ptr = reinterpret_cast<char *>(nvm_region->addr);

                reply_sz = req->payload;

                // FIXME: do we need to sanity check the request ?
                ASSERT(req->addr + req->payload < nvm_region->sz);
                memcpy(reply_buf, server_buf_ptr + req->addr, req->payload);
#endif
                r2::compile_fence();
              } break;
              case 2:
                // null case, do nothing
                break;
              default:
                ASSERT(false) << "unknow request: " << req->read
                              << "; 1 for read and 0 for write, 2: do nothing";
              }
            }

            // 2. prepare a dummy reply
            MemBlock reply(buf, sizeof(MsgHeader));
            r2::compile_fence();

            {
              MsgHeader *header = (MsgHeader *)buf;
              header->type = Reply;
              header->sz = reply_sz;
              header->coro_id = coro_id;
            }
            r2::compile_fence();

#if 0
            auto ret =
              incoming_sessions[session_id]->send_unsignaled(reply, mr.key);
            ASSERT(ret == IOCode::Ok) << "send unsignaled err: " << ret.desc
                                      << " with session id:" << session_id;
#else
            if (0) {
              auto ret = reply_s->send_unsignaled_doorbell(
                  doorbell, reply, mr.key,
                  incoming_sessions[session_id]->my_wr());
              ASSERT(ret == IOCode::Ok) << "send unsignaled err: " << ret.desc;
            }
            // RDMA_LOG(4) << "reply: " << counter;
#endif
          } break;
          default:
            RDMA_ASSERT(false) << "unknown msg type: " << header->type;
          }
        }
        // auto ret_flush = reply_s->flush_a_doorbell(doorbell);
        // ASSERT(ret_flush == IOCode::Ok) << "error: " << ret_flush.desc;
      }
    }));
  }

  for (auto &t : threads)
    t->start();

  LOG(2) << "all server threads started";

  sleep(1);

  ctrl.start_daemon();

  for (auto &t : threads) {
    t->join();
  }

  return 0;
}
