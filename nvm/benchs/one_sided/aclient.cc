
#include <gflags/gflags.h>

#include <functional>
#include <sys/sysinfo.h>

#include "rlib/core/lib.hh"
#include "rlib/tests/random.hh"

#include "r2/src/rdma/sop.hh"

#include "../thread.hh"

#include "../statucs.hh"

#include "../../huge_region.hh"

DEFINE_string(addr, "localhost:8888", "Server address to connect to.");
DEFINE_int64(use_nic_idx, 0, "Which NIC to create QP");
DEFINE_int64(reg_nic_name, 73, "The name to register an opened NIC at rctrl.");
DEFINE_int64(reg_mem_name, 73, "The name to register an MR at rctrl.");
DEFINE_uint64(address_space, 4,
              "The random read/write space of the registered memory (in GB)");

DEFINE_int64(threads, 8, "Number of threads to use.");

DEFINE_uint64(coros, 8, "Number of coroutine used per thread.");

DEFINE_int64(id, 0, "");

using namespace rdmaio;
using namespace rdmaio::rmem;
using namespace rdmaio::qp;

using namespace r2;
using namespace r2::rdma;

using namespace test;

using namespace nvm;

static const int per_socket_cores = 12; // TODO!! hard coded
// const int per_socket_cores = 8;//reserve 2 cores

static int socket_0[] = {0,  2,  4,  6,  8,  10, 12, 14, 16, 18, 20, 22,
                         24, 26, 28, 30, 32, 34, 36, 38, 40, 42, 44, 46};

static int socket_1[] = {1,  3,  5,  7,  9,  11, 13, 15, 17, 19, 21, 23,
                         25, 27, 29, 31, 33, 35, 37, 39, 41, 43, 45, 47};

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

// issue RDMA requests in a window
template <usize window_sz>
void rdma_window(RC *qp, u64 *start_buf, FastRandom &rand, u64 &pending_reqs) {

  const auto address_space =
      static_cast<u64>(FLAGS_address_space) * (1024 * 1024 * 1024L);

  u64 *buf = start_buf;
  for (uint i = 0; i < window_sz; ++i) {
    // RDMA_LOG(4)  << "send one: " << pending_reqs;
    // sleep(1);
    buf[i] = rand.next();
    u64 remote_addr = rand.next() % (address_space - 64);
    //    RDMA_LOG(2) << "read addr: " << remote_addr; sleep(1);
    // remote_addr = 0;

#if 0 // write case
    auto res_s = qp->send_normal(
        {.op = IBV_WR_RDMA_WRITE,
         .flags = IBV_SEND_INLINE | ((pending_reqs == 0) ? (IBV_SEND_SIGNALED) : 0),
         .len = sizeof(u64),
         .wr_id = 0},
        {.local_addr = reinterpret_cast<RMem::raw_ptr_t>(&(buf[i])),
         .remote_addr = remote_addr,
         .imm_data = 0});
#else
    auto res_s = qp->send_normal(
        {.op = IBV_WR_RDMA_READ,
         .flags = 0 | ((pending_reqs == 0) ? (IBV_SEND_SIGNALED) : 0),
         .len = 8,
         .wr_id = 0},
        {.local_addr = reinterpret_cast<RMem::raw_ptr_t>(&(buf[i])),
         .remote_addr = remote_addr,
         .imm_data = 0});
#endif
    RDMA_ASSERT(res_s == IOCode::Ok) << " error: " << res_s.desc;

    if (pending_reqs >= 32) {
      auto res_p = qp->wait_one_comp();
      RDMA_ASSERT(res_p == IOCode::Ok)
          << "wait completion error: " << ibv_wc_status_str(res_p.desc.status);

      pending_reqs = 0;
    } else
      pending_reqs += 1;
  }
}

int main(int argc, char **argv) {

  gflags::ParseCommandLineFlags(&argc, &argv, true);

  RDMA_LOG(4) << "client bootstrap with " << FLAGS_threads << " threads";

  std::vector<gflags::CommandLineFlagInfo> all_flags;
  gflags::GetAllFlags(&all_flags);
  for (auto f : all_flags) {
    if (f.current_value.size())
      RDMA_LOG(2) << f.name << ": " << f.current_value;
  }

  using TThread = Thread<int>;
  std::vector<std::unique_ptr<TThread>> threads;
  std::vector<Statics> statics(FLAGS_threads);

  auto address_space =
      static_cast<u64>(FLAGS_address_space) * (1024 * 1024 * 1024L);

  RCtrl ctrl(6666); //
  {
    // for(uint i = 0;i < RNicInfo::query_dev_names().size();++i)
    //      RDMA_ASSERT(ctrl.opened_nics.reg(i,RNic::create(RNicInfo::query_dev_names().at(i)).value()));
    // auto nic =
    //      RNic::create(RNicInfo::query_dev_names().at(FLAGS_use_nic_idx)).value();

    // register the nic with name 0
    // RDMA_ASSERT(ctrl.opened_nics.reg(FLAGS_reg_nic_name, nic));
  }

  auto huge_region = HugeRegion::create(address_space).value();
  // auto huge_region = std::make_shared<DRAMRegion>(address_space);

  auto rmem = std::make_shared<RMem>( // nvm_region->size(),
      huge_region->size(),
      [&huge_region](u64 s) -> RMem::raw_ptr_t {
        return huge_region->start_ptr();
      },
      [](RMem::raw_ptr_t ptr) {
        // currently we donot free the resource, since the region will use
        // to the end
      });
  // RDMA_ASSERT(ctrl.registered_mrs.create_then_reg(
  // FLAGS_reg_mem_name, rmem,
  // ctrl.opened_nics.query(FLAGS_reg_nic_name).value()));

  ctrl.start_daemon();

  RDMA_LOG(4) << "asym client daemon starts";

  for (uint thread_id = 0; thread_id < FLAGS_threads; ++thread_id) {

    threads.push_back(std::make_unique<TThread>(
        [thread_id, address_space, rmem, &statics, &ctrl]() -> int {
          BindToCore(thread_id);
          // 1. create a local QP to use
          // below are platform specific opts
          auto idx = 1;
          if (thread_id >= 12)
            idx -= 1;
          auto nic = RNic::create(RNicInfo::query_dev_names().at(idx)).value();
          RDMA_ASSERT(ctrl.opened_nics.reg(thread_id, nic));

          auto qp = RC::create(nic, QPConfig()).value();

          // 2. create the pair QP at
          // server using CM
          ConnectManager cm(FLAGS_addr);
          // RDMA_LOG(2) << "start to
          // connect to server: " <<
          // FLAGS_addr;
          auto wait_res = cm.wait_ready(5000000, 4);
          if (wait_res == IOCode::Timeout) // wait 1
                                           // second for
                                           // server to
                                           // ready,
                                           // retry 2
                                           // times
            RDMA_ASSERT(false) << "cm connect to "
                                  "server timeout "
                               << wait_res.desc;

          // 3. create the local MR for
          // usage, and create the
          // remote MR for usage
          auto local_mr = RegHandler::create(rmem, nic).value();
          RDMA_ASSERT(ctrl.registered_mrs.reg(thread_id, local_mr));

          u64 key = 0;
          do {
            auto qp_res = cm.cc_rc(FLAGS_addr + ":" + std::to_string(thread_id),
                                   qp, thread_id,
                                   // idx,
                                   QPConfig());
            // RDMA_ASSERT(qp_res ==
            // IOCode::Ok) << "failed to
            // create remote qp: "
            //                                          <<
            //                                          std::get<0>(qp_res.desc);

            if (qp_res == IOCode::Ok)
              break;
          } while (true);

          // rmem::RegAttr remote_attr;
          do {
            auto fetch_res = cm.fetch_remote_mr(thread_id);
            // RDMA_ASSERT(fetch_res == IOCode::Ok) <<
            // std::get<0>(fetch_res.desc);
            if (fetch_res == IOCode::Ok) {

              auto remote_attr = std::get<1>(fetch_res.desc);
              qp->bind_remote_mr(remote_attr);
              qp->bind_local_mr(local_mr->get_reg_attr().value());

              break;
            }

          } while (true);

          // the benchmark code

          u64 bench_ops = 1000;
          std::string temp_str = FLAGS_addr;
          FastRandom rand(0xdeadbeaf + FLAGS_id * 0xdddd + thread_id +
                          std::hash<std::string>{}(temp_str));
          r2::Timer timer;

          // RDMA_LOG(4) << "all done, start bench code!";

          u64 sum = 0;
          u64 pending_reqs = 0;

          u64 *test_buf = (u64 *)((char *)(rmem->raw_ptr) + thread_id * 40960);

          SScheduler ssched;
          for (uint i = 0; i < FLAGS_coros; ++i) {
            ssched.spawn([test_buf, qp, thread_id, &sum, &statics,
                          &rand](R2_ASYNC) {
              r2::Timer timer;
              SROp op;

              const auto address_space =
                  static_cast<u64>(FLAGS_address_space) * (1024 * 1024 * 1024L);

              char *local_buf = (char *)test_buf + R2_COR_ID() * 4096;

              while (timer.passed_sec() < 100) {
              //while(1) {
                u64 remote_addr = rand.next() % (address_space - 64);
                op.set_read()
                    .set_payload(local_buf, sizeof(u64))
                    .set_remote_addr(remote_addr);
                auto ret = op.execute(qp, IBV_SEND_SIGNALED, R2_ASYNC_WAIT);
                ASSERT(ret == IOCode::Ok);

                sum += local_buf[0];

                statics[thread_id].inc(1);
                R2_YIELD;
              }
              if (R2_COR_ID() == FLAGS_coros)
                R2_STOP();
              R2_RET;
            });
          }
          ssched.run();
#if 0
      while (timer.passed_sec() < 100) {
        /*This is the example code usage of the fully created RCQP */
        // u64 *test_buf = (u64 *)(local_mem->raw_ptr);

        rdma_window<10>(qp.get(), test_buf, rand, pending_reqs);

        statics[thread_id].inc(10);
      }
#endif
          if (thread_id == 0)
            RDMA_LOG(4) << "final check sum: " << sum;

          return 0;
        }));

    /***********************************************************/

    // finally, some clean up, to delete my created QP at server
    // auto del_res = cm.delete_remote_rc(73, key);
    // RDMA_ASSERT(del_res == IOCode::Ok)
    //<< "delete remote QP error: " << del_res.desc;
  }
  for (auto &t : threads)
    t->start();

  sleep(1);
  Reporter::report_thpt(statics, 200);

  RDMA_LOG(4) << "client returns";

  return 0;
}
