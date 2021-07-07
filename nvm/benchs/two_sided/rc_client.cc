#include <gflags/gflags.h>

#include "rlib/core/nicinfo.hh"
#include "rlib/core/qps/recv_iter.hh"
#include "rlib/core/utils/marshal.hh"
#include <gflags/gflags.h>

#include "rlib/core/lib.hh"

#include "rlib/tests/random.hh"

#include "r2/src/msg/rc_session.hh"
#include "r2/src/libroutine.hh"

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
    BindToCore(thread_id);
#if 1
    int idx = 0;
      // if (thread_id >= per_socket_cores)
      //        idx += 1;
#else
    int idx = 1;
    if (thread_id >= per_socket_cores)
      idx -= 1;
#endif
    /**
     * Tedius setup process
     */
    auto nic = RNic::create(RNicInfo::query_dev_names().at(idx)).value();
    auto mem_region = HugeRegion::create(64 * 1024 * 1024).value();
    auto mem = mem_region->convert_to_rmem().value();
    auto handler = RegHandler::create(mem, nic).value();

    Arc<AbsRecvAllocator> alloc = std::make_shared<SimpleAllocator>(
        mem, handler->get_reg_attr().value().key);

    auto recv_cq_res = ::rdmaio::qp::Impl::create_cq(nic, 128);
    ASSERT(recv_cq_res == IOCode::Ok);
    auto recv_cq = std::get<0>(recv_cq_res.desc);

    auto qp = RC::create(nic, QPConfig(), recv_cq).value();

    ConnectManager cm(FLAGS_addr);
    if (cm.wait_ready(1000000, 2) ==
        IOCode::Timeout) // wait 1 second for server to ready, retry 2 times
      assert(false);

    int my_id = FLAGS_id * 73 + thread_id;
    auto qp_res = cm.cc_rc_msg(std::to_string(my_id), std::to_string(thread_id),
                               4096, qp, thread_id, QPConfig());
    RDMA_ASSERT(qp_res == IOCode::Ok) << std::get<0>(qp_res.desc);

    // fetch MRs
    auto fetch_res = cm.fetch_remote_mr(thread_id);
    RDMA_ASSERT(fetch_res == IOCode::Ok) << std::get<0>(fetch_res.desc);
    auto attr = std::get<1>(fetch_res.desc);
    qp->bind_remote_mr(attr);

    auto mem_s = Arc<RMem>(
        new RMem(4096 * FLAGS_coros * 1024)); // allocate a memory with 4M bytes
    auto handler_s = RegHandler::create(mem_s, nic).value();
    auto mr_s = handler_s->get_reg_attr().value();

    // qp->bind_local_mr(handler_s->get_reg_attr().value());
    qp->bind_local_mr(handler->get_reg_attr().value());

    // post recvs
    auto recv_rs = RecvEntriesFactoryv2<128>::create(alloc, 4096);
    auto res = qp->post_recvs(*recv_rs, 128);
    RDMA_ASSERT(res == IOCode::Ok);

    RCRecvSession<128> rss(qp, recv_rs);

    /**
     * Tedius setups done
     */

    RCSession rc_session(my_id, qp);

    SScheduler ssched;
    // pending replies of corutines
    std::vector<usize> wait_replies(2 + FLAGS_coros, 0);
    std::vector<char *> reply_bufs(2 + FLAGS_coros, nullptr);

    ibv_wc *wcs = new ibv_wc[4096];

    /*
     * the future for receiving replies from the server
     * if the expected replies of a coroutine is all received
     * then this coroutine is add back to scheduler
     */

    usize counter = 0;

    poll_func_t reply_future =
        [&rc_session, &ssched, &wait_replies, &recv_cq, &rss, wcs, &counter,
         mem, &reply_bufs]() -> Result<std::pair<::r2::Routine::id_t, usize>> {
      // iterate through all the pending messages
      for (RecvIter<RC, 4096> iter(recv_cq, wcs); iter.has_msgs();
           iter.next()) {
        auto imm_msg = iter.cur_msg().value();

        auto session_id = std::get<0>(imm_msg);
        ASSERT(session_id == 123);
        auto buf = static_cast<char *>(std::get<1>(imm_msg));

        MemBlock msg(buf, 4096);

        MsgHeader *header = msg.interpret_as<MsgHeader>();
        RDMA_ASSERT(header != nullptr);

        auto coro_id = header->coro_id;

        RDMA_ASSERT(header->type == Reply)
            << "recv incorrect reply: " << header->type;

        ASSERT(wait_replies[coro_id] > 0)
            << "recv a coroid: " << coro_id
            << "whose waited reply is: " << wait_replies[coro_id];

        wait_replies[coro_id] -= 1;
        memcpy(reply_bufs[coro_id], msg.interpret_as<char *>(sizeof(MsgHeader)),
               header->sz);

        if (likely(wait_replies[coro_id] == 0)) {
          ssched.addback_coroutine(coro_id);
        }
        rss.consume_one();
        // end receiving all msgs
      }
      // this future shall never return
      return NotReady(std::make_pair<::r2::Routine::id_t, usize>(0u, 0u));
    };

    ssched.emplace_for_routine(0, 0, reply_future);

    FastRandom rand(0xdeadbeaf + FLAGS_id * 0xdddd + thread_id);

    ssched.spawn([handler, &rc_session, thread_id, &reply_bufs, mem, &rand,
                  mem_s, alloc, &wait_replies, &statics](R2_ASYNC) {
      char *send_buf = (char *)(std::get<0>(alloc->alloc_one(4096).value()));

      // connect to the server
      {
        MsgHeader *header = (MsgHeader *)(send_buf);
        *header = {.type = Connect,
                   .magic = 73,
                   .coro_id = R2_COR_ID(),
                   .sz = sizeof(ConnectReq2)};
        ConnectReq2 *req = (ConnectReq2 *)(send_buf + sizeof(MsgHeader));
        req->attr = handler->get_reg_attr().value();

        auto res_s = rc_session.send_unsignaled(
            {.mem_ptr = (void *)(send_buf),
             .sz = sizeof(MsgHeader) + sizeof(ConnectReq2)});
        ASSERT(res_s == IOCode::Ok);

        char reply_buf[64];
        assert(wait_replies.size() > R2_COR_ID());
        wait_replies[R2_COR_ID()] = 1;
        reply_bufs[R2_COR_ID()] = reply_buf;

        auto ret = R2_PAUSE_AND_YIELD;
        ASSERT(ret == IOCode::Ok);
      }

      // connect done, we start coroutines
      for (uint i = 0; i < FLAGS_coros; ++i) {
        R2_EXECUTOR.spawn([&rand, &rc_session, &statics, i, alloc, mem_s, mem,
                           &reply_bufs, &wait_replies, thread_id](R2_ASYNC) {
          assert(wait_replies.size() > R2_COR_ID());

          char *local_buf =
              (char *)(std::get<0>(alloc->alloc_one(4096).value()));
          // char *local_buf = (char *)(mem->raw_ptr);

          const usize window_sz = 1;
          static_assert(window_sz <= 10, "");

          const auto address_space =
              static_cast<u64>(FLAGS_address_space) * (1024 * 1024 * 1024L);

          char *reply_buf = new char[window_sz * 4096];

          while (1) {
            for (uint i = 0; i < window_sz; ++i) {

              MsgHeader *header = (MsgHeader *)local_buf;

              *header = {.type = Req,
                         .magic = 73,
                         .coro_id = R2_COR_ID(),
                         .sz = sizeof(Request)};
              {
                // fill in the request payload
                Request *req = (Request *)((char *)header + sizeof(MsgHeader));
                //*req = {.payload = FLAGS_payload,
                //.addr = align<u32>(rand.next() % address_space, 64),
                //.read = static_cast<u8>((FLAGS_use_read) ? 1 : 0)};
              }

              auto ret = rc_session.send_unsignaled(
                  {.mem_ptr = (void *)(local_buf),
                   .sz = sizeof(MsgHeader) + sizeof(Request)});
              ASSERT(ret == IOCode::Ok);
            }
            wait_replies[R2_COR_ID()] = window_sz;
            reply_bufs[R2_COR_ID()] = reply_buf;

            auto ret = R2_PAUSE_AND_YIELD;
            ASSERT(ret == IOCode::Ok);
            statics[thread_id].inc(window_sz);
          }

          R2_RET;
        });
      }
      R2_RET;
    });
    ssched.run();
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
