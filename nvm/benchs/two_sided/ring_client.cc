#include <gflags/gflags.h>

#include "r2/src/libroutine.hh"
#include "r2/src/rdma/sop.hh"
#include "r2/src/ring_msg/mod.hh"

#include "../gen_addr.hh"

#include "../../huge_region.hh"
#include "../latency.hh"
#include "../statucs.hh"
#include "../thread.hh"

#include "val19.hh"

#include "./proto.hh"

#include "./constants.hh"

DEFINE_string(addr, "localhost:8888", "Server address to connect to.");
DEFINE_int64(use_nic_idx, 0, "Which NIC to create QP");

DEFINE_int64(threads, 8, "Number of threads to use.");

DEFINE_int64(payload, 256, "Number of payload to read/write");

DEFINE_uint64(address_space, 8,
              "The random read/write space of the registered memory (in GB)");

DEFINE_uint64(window_sz, 10, "The window sz of each coroutine.");

DEFINE_int64(id, 0, "");

DEFINE_uint64(coros, 8, "Number of coroutine used per thread.");

DEFINE_bool(use_read, true, "");
DEFINE_bool(random, true, "");

DEFINE_bool(round_up, false, "");
DEFINE_uint32(round_payload, 256, "Roundup of the write payload");

using namespace nvm;

using namespace r2;
using namespace r2::ring_msg;
using namespace r2::rdma;

using namespace rdmaio;

template <typename T>
static constexpr T round_up(const T &num, const T &multiple) {
  assert(multiple && ((multiple & (multiple - 1)) == 0));
  return (num + multiple - 1) & -multiple;
}

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

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  RDMA_LOG(4) << "Msg Ring client bootstrap with " << FLAGS_threads
              << " threads";
  std::vector<gflags::CommandLineFlagInfo> all_flags;
  gflags::GetAllFlags(&all_flags);
  for (auto f : all_flags) {
    if (f.current_value.size())
      RDMA_LOG(2) << f.name << ": " << f.current_value;
  }

  using TThread = Thread<int>;
  std::vector<std::unique_ptr<TThread>> threads;
  std::vector<Statics> statics(FLAGS_threads);

  using RI = RingRecvIter<ring_entry, ring_sz, max_msg_sz>;

  auto address_space =
      static_cast<u64>(FLAGS_address_space) * (1024 * 1024 * 1024L) -
      FLAGS_payload;

  for (uint thread_id = 0; thread_id < FLAGS_threads; ++thread_id) {

    threads.push_back(std::make_unique<TThread>([thread_id, &statics,
                                                 address_space]() -> int {
                                                  //BindToCore(thread_id);
      int idx = 0;
      while (idx >= RNicInfo::query_dev_names().size())
        idx -= 1;
      auto nic = RNic::create(RNicInfo::query_dev_names().at(idx)).value();
      auto mem_region = HugeRegion::create(64 * 1024 * 1024).value();
      // auto mem_region = DRAMRegion::create(64 * 1024 * 1024).value();
      auto mem = mem_region->convert_to_rmem().value();
      auto handler = RegHandler::create(mem, nic).value();

      auto alloc = Arc<SimpleAllocator>(
          new SimpleAllocator(mem, handler->get_reg_attr().value()));

      auto recv_cq_res = ::rdmaio::qp::Impl::create_cq(nic, 128);
      RDMA_ASSERT(recv_cq_res == IOCode::Ok);
      auto recv_cq = std::get<0>(recv_cq_res.desc);

      auto receiver =
          std::make_shared<Receiver<ring_entry, ring_sz, max_msg_sz>>("xxz",
                                                                      recv_cq);

      auto ss = std::make_shared<Session<ring_entry, ring_sz, max_msg_sz>>(
          static_cast<u16>(FLAGS_id * 73 + thread_id), nic, QPConfig(), recv_cq,
          alloc);
      receiver->reg_channel(ss);

      auto cm_res = CMFactory<RingCM>::create(FLAGS_addr, 10000000, 4);
      ASSERT(cm_res == IOCode::Ok);
      auto wait_res = cm_res.desc->wait_ready(2000000, 12);
      if (wait_res ==
          IOCode::Timeout) // wait 1 second for server to ready, retry 2 times
        RDMA_ASSERT(false) << "cm connect to server timeout " << wait_res.desc;

      auto res_c = ss->connect(*(cm_res.desc), std::to_string(thread_id),
                               thread_id, QPConfig());

      ASSERT(res_c == IOCode::Ok) << "res_c error: " << res_c.code.name() << " at client: " << thread_id << "; at server"
                                  << FLAGS_addr;

      LOG(4) << "connect done @" << thread_id;
      // try send one

      SScheduler ssched;
      // pending replies of corutines
      std::vector<usize> wait_replies(2 + FLAGS_coros, 0);
      std::vector<char *> reply_bufs(2 + FLAGS_coros, nullptr);

      poll_func_t reply_future =
          [&receiver, &wait_replies, &ssched,
           &reply_bufs]() -> Result<std::pair<::r2::Routine::id_t, usize>> {
        for (RI iter(receiver); iter.has_msgs(); iter.next()) {
          auto msg = iter.cur_msg();

          MsgHeader *header = msg.interpret_as<MsgHeader>();

          RDMA_ASSERT(header != nullptr);

          auto coro_id = header->coro_id;

          RDMA_ASSERT(header->type == Reply)
              << "recv incorrect reply: " << header->type;

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
      LOG(4) << "emplace future done @" << thread_id;

      ::test::FastRandom rand(0xdeadbeaf + FLAGS_id * 0xdddd + thread_id);

#if 0
      const u64 four_h_mb = 400 * 1024 * 1024;  // 400 MB is taken from xx
      RandomAddr rgen(four_h_mb, four_h_mb * thread_id); // random generator
      SeqAddr sgen(four_h_mb, four_h_mb * thread_id);    // sequential generator
#else

      const u64 four_h_mb = address_space / FLAGS_threads;
      ASSERT(four_h_mb > 0);

      RandomAddr rgen(address_space, 0);              // random generator
      SeqAddr sgen(four_h_mb, four_h_mb * thread_id); // sequential generator
#endif

      for (uint i = 0; i < FLAGS_coros; ++i) {
        ssched.spawn([&ss,
                      alloc,
                      &statics,
                      &wait_replies,
                      &reply_bufs,
                      &rand,
                      address_space,
                      &rgen,
                      &sgen,
                      thread_id](R2_ASYNC) {
          r2::Timer t;

          char *local_buf = (char *)(std::get<0>(
              alloc->alloc_one(8192 * (FLAGS_window_sz + 1)).value()));

          const usize window_sz = FLAGS_window_sz;
          // static_assert(window_sz <= 10, "");

          char *reply_buf = new char[window_sz * 4096];

//          FlatLatRecorder lats; // used to record lat
          t.reset();

          while (1) {
            r2::compile_fence();
            if (thread_id == 0)
              t.reset();

            for (uint i = 0; i < window_sz; ++i) {
              // u64 addr = rand.next() % (four_h_mb) + four_h_mb * thread_id;
              u64 addr = 0;
              if (FLAGS_random)
                addr = rgen.gen(rand);
              else
                addr = sgen.gen(FLAGS_payload);

              if (FLAGS_round_up) {
                addr = round_up<usize>(addr, FLAGS_round_payload);
              }

              if (addr + FLAGS_payload >= address_space) {
                addr = address_space - FLAGS_payload;
              }


              char *msg_buf = local_buf + 8192 * i;
              MsgHeader *header = (MsgHeader *)(msg_buf);
              *header = {.type = Req,
                         .magic = 73,
                         .coro_id = R2_COR_ID(),
                         .sz = sizeof(Request)};
              {
                // fill in the request payload
                Request *req = (Request *)((char *)header + sizeof(MsgHeader));
                *req = {.payload = FLAGS_payload,
                        .addr = addr,
                        .read = static_cast<u8>((FLAGS_use_read) ? 1 : 0)};
              }

              auto ret = ss->send_unsignaled(
                  {.mem_ptr = (void *)(msg_buf),
                   .sz = sizeof(MsgHeader) + sizeof(Request) +
                         (FLAGS_use_read ? 0 : FLAGS_payload)});
              ASSERT(ret == IOCode::Ok);

            }

            wait_replies[R2_COR_ID()] = window_sz;
            reply_bufs[R2_COR_ID()] = reply_buf;

            auto ret = R2_PAUSE_AND_YIELD;
            ASSERT(ret == IOCode::Ok);

            // record the latency
            if (thread_id == 0) {
//              lats.add_one(t.passed_msec());
//              statics[thread_id].float_data = lats.get_lat();
            }

            statics[thread_id].inc(window_sz);
          }

          R2_RET;
        });
      }

      // run all the coroutines
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

  return 0;
}
