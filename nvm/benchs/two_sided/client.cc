#include <gflags/gflags.h>

#include <assert.h>

#include "rlib/core/lib.hh"
#include "rlib/core/qps/doorbell_helper.hh"
#include "rlib/core/qps/mod.hh"
#include "rlib/core/qps/recv_iter.hh"

#include "rlib/tests/random.hh"

#include "r2/src/msg/ud_session.hh"

#include "../../huge_region.hh"
#include "../statucs.hh"
#include "../thread.hh"

#include "val19.hh"

#include "./proto.hh"

using namespace rdmaio;
using namespace rdmaio::qp;
using namespace rdmaio::rmem;
using namespace test;

using namespace nvm;

using namespace r2;

DEFINE_string(addr, "localhost:8888", "Server address to connect to.");
DEFINE_int64(use_nic_idx, 0, "Which NIC to create QP");

DEFINE_int64(threads, 8, "Number of threads to use.");

DEFINE_int64(payload, 256, "Number of payload to read/write");

DEFINE_uint64(address_space, 1,
              "The random read/write space of the registered memory (in GB)");

DEFINE_int64(id, 0, "");

DEFINE_uint64(coros, 8, "Number of coroutine used per thread.");

DEFINE_bool(use_read, true, "");

template <typename Nat> Nat align(const Nat &x, const Nat &a) {
  auto r = x % a;
  return r ? (x + a - r) : x;
}

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
    y = socket_1[x + (mac_num - mac_per_node / 2) * nthreads];
  }
#else
  // bind ,andway
  if (x >= per_socket_cores) {
    // there is no other cores in the first socket
    y = socket_1[x - per_socket_cores];
  } else {
    y = socket_0[x];
  }

#endif

  // fprintf(stdout,"worker: %d binding %d\n",x,y);
  cpu_set_t mask;
  CPU_ZERO(&mask);
  CPU_SET(y, &mask);
  sched_setaffinity(0, sizeof(mask), &mask);

  return 0;
}

// an allocator to allocate recv buffer for messages
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
    if (total_mem < sz)
      return {};
    auto ret = buf;
    buf = static_cast<char *>(buf) + sz;
    total_mem -= sz;
    return std::make_pair(ret, key);
  }

  ::rdmaio::Option<std::pair<rmem::RMem::raw_ptr_t, rmem::RegAttr>>
  alloc_one_for_remote(const usize &sz) override {
    return {};
  }
};

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  RDMA_LOG(4) << "Msg client bootstrap with " << FLAGS_threads << " threads";

  std::vector<gflags::CommandLineFlagInfo> all_flags;
  gflags::GetAllFlags(&all_flags);
  for (auto f : all_flags) {
    if (f.current_value.size())
      RDMA_LOG(2) << f.name << ": " << f.current_value;
  }

  using TThread = Thread<int>;
  std::vector<std::unique_ptr<TThread>> threads;
  std::vector<Statics> statics(FLAGS_threads);

  // auto address_space =
  // static_cast<u64>(FLAGS_address_space) * (1024 * 1024 * 1024L);

  for (uint thread_id = 0; thread_id < FLAGS_threads; ++thread_id) {

    threads.push_back(std::make_unique<TThread>([thread_id, &statics]() -> int {
    //                                                  BindToCore(thread_id +
    //                                                  per_socket_cores);

                                                  auto idx = FLAGS_use_nic_idx;
      auto nic = RNic::create(RNicInfo::query_dev_names().at(idx)).value();
      // prepare the message buf
      // auto mem = Arc<RMem>(
      // new RMem(64 * 1024 * 1024)); // allocate a memory with 4M bytes

      auto mem_region = HugeRegion::create(64 * 1024 * 1024).value();
      auto mem = mem_region->convert_to_rmem().value();

      auto handler = RegHandler::create(mem, nic).value();
      SimpleAllocator alloc(mem, handler->get_reg_attr().value().key);

      // prepare buffer, contain 16 recv entries, each has 4096 bytes
      auto recv_rs =
          RecvEntriesFactory<SimpleAllocator, 2048, 4096>::create(alloc);
      // ctrl.registered_mrs.reg(FLAGS_reg_mem_name, handler);

      auto ud = UD::create(nic, QPConfig()).value();

      // post these recvs to the UD
      {
        recv_rs->sanity_check();
        auto res = ud->post_recvs(*recv_rs, 2048);
        RDMA_ASSERT(res == IOCode::Ok);
      }

      ConnectManager cm(FLAGS_addr);

      auto wait_res = cm.wait_ready(1000000, 4);
      if (wait_res ==
          IOCode::Timeout) // wait 1 second for server to ready, retry 2 times
        RDMA_ASSERT(false) << "cm connect to server timeout " << wait_res.desc;

      ::rdmaio::qp::QPAttr ud_attr;
#if 1
      do {
        auto fetch_qp_attr_res = cm.fetch_qp_attr(std::to_string(thread_id));
        // RDMA_ASSERT(fetch_qp_attr_res == IOCode::Ok) <<
        // std::get<0>(fetch_qp_attr_res.desc);
        if (fetch_qp_attr_res == IOCode::Ok) {
          ud_attr = std::get<1>(fetch_qp_attr_res.desc);
          break;
        }
        RDMA_ASSERT(fetch_qp_attr_res != IOCode::Err);
        sleep(1);
      } while (1);
#else
      ud_attr = ud->my_attr();
#endif
      RDMA_LOG(4) << "attr: " << ud_attr.qpn << " " << ud_attr.qkey;

      r2::Timer t;

      auto ud_session =
          UDSession::create(thread_id + 73 * FLAGS_id, ud, ud_attr).value();

      FastRandom rand(0xdeadbeaf + FLAGS_id * 0xdddd + thread_id);

      // scheduler for executing all coroutines
      SScheduler ssched;

      // pending replies of corutines
      std::vector<usize> wait_replies(2 + FLAGS_coros, 0);
      std::vector<char *> reply_bufs(2 + FLAGS_coros, nullptr);

      /*!
        Spawn a future to receive replies from the server.
        If the #of replies received by each corotine is equal to the expected
        replies, the coroutine are waken up for the execution.
       */
      poll_func_t reply_future =
          [&ud, &recv_rs, &ssched, &wait_replies,
           &reply_bufs]() -> Result<std::pair<::r2::Routine::id_t, usize>> {
        // iterate through all the pending messages
        for (RecvIter<UD, 2048> iter(ud, recv_rs); iter.has_msgs();
             iter.next()) {
          auto imm_msg = iter.cur_msg().value();

          auto session_id = std::get<0>(imm_msg);
          auto buf = static_cast<char *>(std::get<1>(imm_msg)) + kGRHSz;

          MemBlock msg(buf, 4096 - kGRHSz);

          MsgHeader *header = msg.interpret_as<MsgHeader>();
          RDMA_ASSERT(header != nullptr);
          RDMA_ASSERT(header->type == Reply);

          auto coro_id = header->coro_id;
          // RDMA_LOG(4) << "Recv a coro: " << coro_id << " with payload: " <<
          // header->sz
          //<< "cur: " << wait_replies[coro_id];

          ASSERT(wait_replies[coro_id] > 0)
              << "recv a coroid: " << coro_id
              << "whose waited reply is: " << wait_replies[coro_id];

          wait_replies[coro_id] -= 1;
          memcpy(reply_bufs[coro_id],
                 msg.interpret_as<char *>(sizeof(MsgHeader)), header->sz);

          if (likely(wait_replies[coro_id] == 0)) {
            ssched.addback_coroutine(coro_id);
          }
        }
        // this future shall never return
        return NotReady(std::make_pair<::r2::Routine::id_t, usize>(0u, 0u));
      };

      ssched.emplace_for_routine(0, 0, reply_future);

      auto mem_s = Arc<RMem>(new RMem(4096 * FLAGS_coros *
                                      1024)); // allocate a memory with 4M bytes
      auto handler_s = RegHandler::create(mem_s, nic).value();
      auto mr_s = handler_s->get_reg_attr().value();

      ssched.spawn([ud, mem_s, mr_s, ud_session, thread_id, &statics,
                    &reply_bufs, &wait_replies, &rand](R2_ASYNC) {
        //
        char *send_buf = (char *)(mem_s->raw_ptr);

        // connect to the server
        {
#if 1
          char *send_buf = ((char *)(mem_s->raw_ptr));
          MsgHeader *header = (MsgHeader *)(send_buf);
          *header = {.type = Connect,
                     .magic = 73,
                     .coro_id = R2_COR_ID(),
                     .sz = sizeof(ConnectReq)};
          ConnectReq *req = (ConnectReq *)(send_buf + sizeof(MsgHeader));
          req->attr = ud->my_attr();

          auto res_s = ud_session->send_unsignaled(
              {.mem_ptr = (void *)(send_buf),
               .sz = sizeof(MsgHeader) + sizeof(ConnectReq)},
              mr_s.key);
          ASSERT(res_s == IOCode::Ok);

          char reply_buf[1024];
          assert(wait_replies.size() > R2_COR_ID());
          wait_replies[R2_COR_ID()] = 1;
          reply_bufs[R2_COR_ID()] = reply_buf;

          auto ret = R2_PAUSE_AND_YIELD;
          ASSERT(ret == IOCode::Ok);

          ConnectReply *reply = (ConnectReply *)reply_buf;
          ASSERT(reply->session_id == ud_session->my_id())
              << "reply sesson id:" << reply->session_id;
          ASSERT(reply->coro_id == R2_COR_ID());
#else
#endif
        }

        RDMA_LOG(2) << "start to spawn all routines";

        DoorbellHelper<16> doorbell(IBV_WR_SEND_WITH_IMM);
        // spawn routines
        for (uint i = 0; i < FLAGS_coros; ++i) {
          R2_EXECUTOR.spawn([i, ud_session, thread_id, send_buf, mr_s,
                             &reply_bufs, &wait_replies, &statics, &doorbell,
                             &rand](R2_ASYNC) {
            assert(wait_replies.size() > R2_COR_ID());

            char *local_buf = send_buf + i * 4096;
            MsgHeader *header = (MsgHeader *)local_buf;

            // here is the main evaluation loop
            const usize window_sz = 8;

            const auto address_space =
                static_cast<u64>(FLAGS_address_space) * (1024 * 1024 * 1024L);

            char *reply_buf = new char[window_sz * 4096];

            while (1) {
#if 0
              // use an un-optimized (doorbell optimization) version
              for (uint i = 0; i < window_sz; ++i) {
                *header = {.type = Req,
                           .magic = 73,
                           .coro_id = R2_COR_ID(),
                           .sz = sizeof(Request)};

                {
                  // fill in the request payload
                  Request *req =
                      (Request *)((char *)local_buf + sizeof(MsgHeader));

                  *req = {.payload = FLAGS_payload,
                          .addr = align<u32>(rand.next() % address_space, 64),
                          .read = static_cast<u8>((FLAGS_use_read) ? 1 : 0)};
                }

                auto res_s = ud_session->send_unsignaled(
                    {.mem_ptr = (void *)(local_buf),
                     .sz = sizeof(MsgHeader) + sizeof(Request)},
                    mr_s.key);
                ASSERT(res_s == IOCode::Ok);
              }
#else
              // use doorbell, need to hard code
              assert(doorbell.empty());
              for (uint i = 0; i < window_sz; ++i) {

                *header = {.type = Req,
                           .magic = 73,
                           .coro_id = R2_COR_ID(),
                           .sz = sizeof(Request)};
                {
                  // fill in the request payload
                  Request *req =
                      (Request *)((char *)local_buf + sizeof(MsgHeader));
#if 1
                  *req = {.payload = FLAGS_payload,
                          .addr = align<u32>(rand.next() % address_space, 64),
                          .read = static_cast<u8>((FLAGS_use_read) ? 1 : 0)};
#else
                  *req = {.payload = FLAGS_payload,
                          .addr = rand.next() % address_space,
                          .read = static_cast<u8>(2)};

#endif
                }

                auto ret = ud_session->send_unsignaled_doorbell(
                    doorbell,
                    {.mem_ptr = (void *)(local_buf),
                     .sz = sizeof(MsgHeader) + sizeof(Request)},
                    mr_s.key, ud_session->my_wr());
                ASSERT(ret == IOCode::Ok) << "error: " << ret.desc;
              }
              ASSERT(ud_session->flush_a_doorbell(doorbell) == IOCode::Ok);
#endif
              //wait_replies[R2_COR_ID()] = window_sz;
              //reply_bufs[R2_COR_ID()] = reply_buf;

              //auto ret = R2_PAUSE_AND_YIELD;
              //ASSERT(ret == IOCode::Ok);
              statics[thread_id].inc(window_sz);
              continue;
              R2_YIELD;
            }
            R2_RET;
          });
        }
        // R2_STOP();
        R2_RET;
      });

      ssched.run();

      return 0;
    }));
  }

  for (auto &t : threads)
    t->start();
  LOG(2) << "all thread run";

  Reporter::report_thpt(statics, 40);

  for (auto &t : threads) {
    t->join();
  }

  LOG(2) << "client returns";
  return 0;
}
