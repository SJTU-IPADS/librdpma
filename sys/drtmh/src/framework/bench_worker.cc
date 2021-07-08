#include "rocc_config.h"
#include "tx_config.h"

#include "config.h"

#include "bench_worker.h"

#include "req_buf_allocator.h"

#include "../../nvm/nvm_region.hh"
#include "db/txs/dbrad.h"
#include "db/txs/dbsi.h"

#include "rlib/core/lib.hh"
#include "rlib/core/qps/config.hh"
#include "rlib/core/rctrl.hh"

#include "../../nvm/huge_region.hh"

extern size_t coroutine_num;
extern size_t current_partition;
extern size_t nclients;
extern size_t nthreads;
extern int tcp_port;

extern ::rdmaio::Arc<MemoryRegion> nvm_region;

extern ::rdmaio::RCtrl ctrl;

thread_local ::rdmaio::Arc<::rdmaio::qp::RC> nvm_qp = nullptr;
thread_local ::rdmaio::Arc<::rdmaio::qp::RC> nvm_qp1 = nullptr;
thread_local ::rdmaio::Qp *wrapper_nvm_qp = nullptr;
thread_local ::rdmaio::Qp *wrapper_nvm_qp1 = nullptr;
thread_local ::rdmaio::rmem::RegAttr nvm_mr;
thread_local ::rdmaio::rmem::RegAttr dram_mr;
thread_local ::rdmaio::rmem::RegAttr local_mr;

namespace nocc {

__thread oltp::BenchWorker *worker = NULL;
__thread TXHandler **txs_ = NULL;
__thread rtx::OCC **new_txs_ = NULL;

extern uint64_t total_ring_sz;
extern uint64_t ring_padding;

std::vector<CommQueue *> conns; // connections between clients and servers
extern std::vector<SingleQueue *> local_comm_queues;
extern zmq::context_t send_context;

namespace oltp {

// used to calculate benchmark information ////////////////////////////////
__thread std::vector<size_t> *txn_counts = NULL;
__thread std::vector<size_t> *txn_aborts = NULL;
__thread std::vector<size_t> *txn_remote_counts = NULL;

// used to calculate the latency of each workloads
__thread workload_desc_vec_t *workloads;

extern char *rdma_buffer;
extern uint64_t r_buffer_size;

extern __thread util::fast_random *random_generator;

// per-thread memory allocator
__thread RPCMemAllocator *msg_buf_alloctors = NULL;
extern MemDB *backup_stores_[MAX_BACKUP_NUM];

SpinLock exit_lock;

BenchWorker::BenchWorker(unsigned worker_id, bool set_core, unsigned seed,
                         uint64_t total_ops, spin_barrier *barrier_a,
                         spin_barrier *barrier_b, BenchRunner *context,
                         DBLogger *db_logger)
    : RWorker(worker_id, cm, seed), initilized_(false), set_core_id_(set_core),
      ntxn_commits_(0), ntxn_aborts_(0), ntxn_executed_(0),
      ntxn_abort_ratio_(0), ntxn_remote_counts_(0), ntxn_strict_counts_(0),
      total_ops_(total_ops), context_(context), db_logger_(db_logger),
      // r-set some local members
      new_logger_(NULL) {
  assert(cm_ != NULL);
  INIT_LAT_VARS(yield);
#if CS == 0
  nclients = 0;
  server_routine = coroutine_num;
#else
  if (nclients >= nthreads)
    server_routine = coroutine_num;
  else
    server_routine = MAX(coroutine_num, server_routine);
    // server_routine = MAX(40,server_routine);
#endif
  // server_routine = 20;
}

void BenchWorker::init_tx_ctx() {

  worker = this;
  txs_ = new TXHandler *[1 + server_routine + 2];
  new_txs_ = new rtx::OCC *[1 + server_routine + 2];
  std::fill_n(new_txs_, 1 + server_routine + 2, static_cast<rtx::OCC *>(NULL));

  // msg_buf_alloctors = new RPCMemAllocator[1 + server_routine];

  txn_counts = new std::vector<size_t>();
  txn_aborts = new std::vector<size_t>();
  txn_remote_counts = new std::vector<size_t>();

  for (uint i = 0; i < NOCC_BENCH_MAX_TX; ++i) {
    txn_counts->push_back(0);
    txn_aborts->push_back(0);
    txn_remote_counts->push_back(0);
  }
  for (uint i = 0; i < 1 + server_routine + 2; ++i)
    txs_[i] = NULL;

  // init workloads
  workloads = new workload_desc_vec_t[server_routine + 2];
}

void BenchWorker::run() {

  // create connections
  exit_lock.Lock();
  if (conns.size() == 0) {
    // only create once
    for (uint i = 0; i < nthreads + nclients + 1; ++i)
      conns.push_back(new CommQueue(nthreads + nclients + 1));
  }
  exit_lock.Unlock();

  // BindToCore(worker_id_); // really specified to platforms
  binding(worker_id_);
  init_tx_ctx();
  init_routines(server_routine);
  // LOG(4) << "bench worker wit " << server_routine << " routines";
  // ASSERT(false);

  // create_logger();

#if USE_RDMA
  init_rdma();
  create_qps();
#endif
#if USE_TCP_MSG == 1
  assert(local_comm_queues.size() > 0);
  create_tcp_connections(local_comm_queues[worker_id_], tcp_port, send_context);
#else
  MSGER_TYPE type;

#if USE_UD_MSG == 1
  type = UD_MSG;
#if LOCAL_CLIENT == 1 || CS == 0
  int total_connections = 1;
#else
  int total_connections = nthreads + nclients;
#endif
  create_rdma_ud_connections(total_connections);
#else
  create_rdma_rc_connections(rdma_buffer + HUGE_PAGE_SZ, total_ring_sz,
                             ring_padding);
#endif

#endif

  this->init_new_logger(backup_stores_);
  this->thread_local_init(); // application specific init

  register_callbacks();

  // register NVM
  auto nic = ::rdmaio::RNic::create(
                 ::rdmaio::RNicInfo::query_dev_names().at(choose_rnic_port()))
                 .value();

  nvm_qp = ::rdmaio::qp::RC::create(nic, ::rdmaio::qp::QPConfig()).value();
  nvm_qp1 = ::rdmaio::qp::RC::create(nic, ::rdmaio::qp::QPConfig()).value();

  int nvm_id = worker_id_ + current_partition * 73;

  MemoryRegion local_region(r_buffer_size, rdma_buffer);
  auto handler =
    RegHandler::create(local_region.convert_to_rmem().value(), nic).value();
  local_mr = handler->get_reg_attr().value();

  //MemoryRegion dram_region(r_, rdma_buffer);
  //DRAMRegion dram_region(16 * 1024 * 1024);
  auto dram_region = HugeRegion::create(16 * 1024 * 1024).value();

  // LOG(4) << "local_region data: " << (void *)rdma_buffer << " " <<
  // r_buffer_size;

  if (nvm_region != nullptr) {
    ASSERT(current_partition == 0);
    // 1. opened anther nic
    ctrl.opened_nics.reg(worker_id_, nic);

    ASSERT(ctrl.registered_mrs.create_then_reg(
        worker_id_ + 73, dram_region->convert_to_rmem().value(), nic));

    ASSERT(ctrl.registered_mrs.create_then_reg(
                                               worker_id_, nvm_region->convert_to_rmem().value(), nic));
  }


  // then we query the pair for the NVM QP
  if (current_partition != 0 && 1) {
    do {
      ConnectManager cm("r740:6666");
      auto wait_res = cm.wait_ready(1000000, 4);
      if (wait_res ==
          IOCode::Timeout) // wait 1 second for server to ready, retry 2 times
        RDMA_ASSERT(false) << "cm connect to server timeout " << wait_res.desc;

      auto fetch_res = cm.fetch_remote_mr(worker_id_);
      if (fetch_res == IOCode::Ok) {
        // 1. fetch nvm mr
        nvm_mr = std::get<1>(fetch_res.desc);

        // 2. fetch dram mr
        auto fetch_res1 = cm.fetch_remote_mr(worker_id_ + 73);
        ASSERT(fetch_res1 == IOCode::Ok) << "dram mr fetch res: ";
        dram_mr = std::get<1>(fetch_res1.desc);

        // connect the RC
        char qp_name[64];
        snprintf(qp_name, 64, "%rc:%d", nvm_id);
        auto qp_res =
            cm.cc_rc(qp_name, nvm_qp, worker_id_, ::rdmaio::qp::QPConfig());


        RDMA_ASSERT(qp_res == IOCode::Ok)
            << std::get<0>(qp_res.desc) << "@ worker: " << worker_id_;

        snprintf(qp_name, 64, "%rc:%d:x", nvm_id);
        qp_res =
            cm.cc_rc(qp_name, nvm_qp1, worker_id_, ::rdmaio::qp::QPConfig());

        break;
      }

    } while (1);

    // finally bind this QP to local/remote MR
    // 1. create the local MR
    ASSERT(rdma_buffer != nullptr);
    //LOG(4) << "local mr key: " << local_mr.key;

    nvm_qp->bind_local_mr(local_mr);
    nvm_qp->bind_remote_mr(nvm_mr);
    // sanity checksy
    auto res_s = nvm_qp->send_normal(
        {.op = IBV_WR_RDMA_WRITE,
         .flags = IBV_SEND_SIGNALED,
         .len = sizeof(u64),
         .wr_id = 0},
        {.local_addr = reinterpret_cast<RMem::raw_ptr_t>(local_region.addr),
         .remote_addr = 0,
         .imm_data = 0});
    ASSERT(res_s == IOCode::Ok);
    auto res_p = nvm_qp->wait_one_comp();
    ASSERT(res_p == IOCode::Ok)
        << "error " << ::rdmaio::qp::RC::wc_status(res_p.desc);

    // 2. convert the QP
    wrapper_nvm_qp = new ::rdmaio::Qp();
    wrapper_nvm_qp1 = new ::rdmaio::Qp();

    wrapper_nvm_qp->send_cq = nvm_qp->cq;
    wrapper_nvm_qp->qp = nvm_qp->qp;

    wrapper_nvm_qp1->send_cq = nvm_qp1->cq;
    wrapper_nvm_qp1->qp = nvm_qp1->qp;

    //wrapper_nvm_qp->remote_attr_.memory_attr_ = qp->remote_attr_.memory_attr_;
    wrapper_nvm_qp->remote_attr_.memory_attr_.buf = dram_mr.buf;
    wrapper_nvm_qp->remote_attr_.memory_attr_.rkey = dram_mr.key;

    wrapper_nvm_qp1->remote_attr_.memory_attr_.buf = nvm_mr.buf;
    wrapper_nvm_qp1->remote_attr_.memory_attr_.rkey = nvm_mr.key;

    //wrapper_nvm_qp->dev_ = qp->dev_;
    wrapper_nvm_qp->dev_ = new RdmaDevice;
    wrapper_nvm_qp->dev_->conn_buf_mr = new ibv_mr;
    wrapper_nvm_qp->dev_->conn_buf_mr->lkey = local_mr.key;

    wrapper_nvm_qp1->dev_ = new RdmaDevice;
    wrapper_nvm_qp1->dev_->conn_buf_mr = new ibv_mr;
    wrapper_nvm_qp1->dev_->conn_buf_mr->lkey = local_mr.key;
  }
  // LOG(4) << "NVM setup done!";

#if CS == 1
#if LOCAL_CLIENT == 0
  create_client_connections(nthreads + nclients);
#endif
  assert(pending_reqs_.empty());
  rpc_->register_callback(
      std::bind(&BenchWorker::req_rpc_handler, this, std::placeholders::_1,
                std::placeholders::_2, std::placeholders::_3,
                std::placeholders::_4),
      RPC_REQ);
#endif
  // waiting for master to start workers
  this->inited = true;
  //LOG(4) << "worker: " << this->worker_id_ << " done!";
#if 1
  while (!this->running) {
    asm volatile("" ::: "memory");
  }
#else
  this->running = true;
#endif
  // starts the new_master_routine
  start_routine(); // uses parent worker->start
}

void __attribute__((
    optimize("O1"))) // this flag is very tricky, it should be set this way
BenchWorker::worker_routine(yield_func_t &yield) {

  assert(conns.size() != 0);

  using namespace db;
  /* worker routine that is used to run transactions */
  workloads[cor_id_] = get_workload();
  auto &workload = workloads[cor_id_];

  // Used for OCC retry
  unsigned int backoff_shifts = 0;
  unsigned long abort_seed = 73;

  while (abort_seed == random_generator[cor_id_].get_seed()) {
    abort_seed += 1; // avoids seed collision
  }

  if (current_partition == 0)
    indirect_must_yield(yield);
#if SI_TX
    // if(current_partition == 0) indirect_must_yield(yield);
#endif

  uint64_t retry_count(0);
  while (true) {
#if CS == 0
    /* select the workload */
    double d = random_generator[cor_id_].next_uniform();

    uint tx_idx = 0;
    for (size_t i = 0; i < workload.size(); ++i) {
      if ((i + 1) == workload.size() || d < workload[i].frequency) {
        tx_idx = i;
        break;
      }
      d -= workload[i].frequency;
    }
#else
#if LOCAL_CLIENT
    REQ req;
    if (!conns[worker_id_]->front((char *)(&req))) {
      yield_next(yield);
      continue;
    }
    conns[worker_id_]->pop();
#else
    if (pending_reqs_.empty()) {
      yield_next(yield);
      continue;
    }
    REQ req = pending_reqs_.front();
    pending_reqs_.pop();
#endif
    int tx_idx = req.tx_id;
#endif

#if CALCULATE_LAT == 1
    if (cor_id_ == 1) {
      // only profile the latency for cor 1
      //#if LATENCY == 1
      latency_timer_.start();
      //#else
      (workload[tx_idx].latency_timer).start();
      //#endif
    }
#endif
    const unsigned long old_seed = random_generator[cor_id_].get_seed();
    (*txn_counts)[tx_idx] += 1;
  abort_retry:
    // LOG(4) << "routine " << cor_id_ << " execute one"; sleep(1);
    ntxn_executed_ += 1;
    auto ret = workload[tx_idx].fn(this, yield);
#if NO_ABORT == 1
    // ret.first = true;
#endif
    // if(current_partition == 0){
    if (likely(ret.first)) {
      // commit case
      retry_count = 0;
#if CALCULATE_LAT == 1
      if (cor_id_ == 1) {
        //#if LATENCY == 1
        latency_timer_.end();
        //#else
        workload[tx_idx].latency_timer.end();
        //#endif
      }
#endif

#if CS == 0 // self_generated requests
      ntxn_commits_ += 1;

#if PROFILE_RW_SET == 1 || PROFILE_SERVER_NUM == 1
      if (ret.second > 0)
        workload[tx_idx].p.process_rw(ret.second);
#endif
#else // send reply to clients
      ntxn_commits_ += 1;
      // reply to client
#if LOCAL_CLIENT
      char dummy = req.cor_id;
      conns[req.c_tid]->enqueue(worker_id_, (char *)(&dummy), sizeof(char));
#else
      char *reply = rpc_->get_reply_buf();
      rpc_->send_reply(reply, sizeof(uint8_t), req.c_id, req.c_tid, req.cor_id,
                       client_handler_);
#endif
#endif
    } else {
      retry_count += 1;
      // if(retry_count > 10000000) assert(false);
      // abort case
      if (old_seed != abort_seed) {
        /* avoid too much calculation */
        ntxn_abort_ratio_ += 1;
        abort_seed = old_seed;
        (*txn_aborts)[tx_idx] += 1;
      }
      ntxn_aborts_ += 1;
      yield_next(yield);

      // reset the old seed
      random_generator[cor_id_].set_seed(old_seed);
      goto abort_retry;
    }
    yield_next(yield);
    // end worker main loop
  }
}

void BenchWorker::exit_handler() {

  if (worker_id_ == 0) {

    // only sample a few worker information
    auto &workload = workloads[1];

    auto second_cycle = BreakdownTimer::get_one_second_cycle();
#if 1
    // exit_lock.Lock();
    fprintf(stdout, "aborts: ");
    workload[0].latency_timer.calculate_detailed();
    fprintf(stdout,
            "%s ratio: %f ,executed %lu, latency %f, rw_size %f, m %f, 90 %f, "
            "99 %f\n",
            workload[0].name.c_str(),
            (double)((*txn_aborts)[0]) /
                ((*txn_counts)[0] + ((*txn_counts)[0] == 0)),
            (*txn_counts)[0],
            workload[0].latency_timer.report() / second_cycle * 1000,
            workload[0].p.report(),
            workload[0].latency_timer.report_medium() / second_cycle * 1000,
            workload[0].latency_timer.report_90() / second_cycle * 1000,
            workload[0].latency_timer.report_99() / second_cycle * 1000);

    for (uint i = 1; i < workload.size(); ++i) {
      workload[i].latency_timer.calculate_detailed();
      fprintf(stdout,
              "        %s ratio: %f ,executed %lu, latency: %f, rw_size %f, m "
              "%f, 90 %f, 99 %f\n",
              workload[i].name.c_str(),
              (double)((*txn_aborts)[i]) /
                  ((*txn_counts)[i] + ((*txn_counts)[i] == 0)),
              (*txn_counts)[i],
              workload[i].latency_timer.report() / second_cycle * 1000,
              workload[i].p.report(),
              workload[i].latency_timer.report_medium() / second_cycle * 1000,
              workload[i].latency_timer.report_90() / second_cycle * 1000,
              workload[i].latency_timer.report_99() / second_cycle * 1000);
    }
    fprintf(stdout, "\n");

    fprintf(stdout, "total: ");
    for (uint i = 0; i < workload.size(); ++i) {
      fprintf(stdout, " %d %lu; ", i, (*txn_counts)[i]);
    }
    fprintf(stdout, "succs ratio %f\n",
            (double)(ntxn_executed_) / (double)(ntxn_commits_));

    exit_report();
#endif
#if RECORD_STALE
    util::RecordsBuffer<double> total_buffer;
    for (uint i = 0; i < server_routine + 1; ++i) {

      // only calculate staleness for timestamp based method
#if RAD_TX
      auto tx = dynamic_cast<DBRad *>(txs_[i]);
#elif SI_TX
      auto tx = dynamic_cast<DBSI *>(txs_[i]);
#else
      DBRad *tx = NULL;
#endif
      assert(tx != NULL);
      total_buffer.add(tx->stale_time_buffer);
    }
    fprintf(stdout, "total %d points recorded\n", total_buffer.size());
    total_buffer.sort_buffer(0);
    std::vector<int> cdf_idx({1,  5,  10, 15, 20, 25, 30, 35, 40, 45,
                              50, 55, 60, 65, 70, 75, 80, 81, 82, 83,
                              84, 85, 90, 95, 97, 98, 99, 100});
    total_buffer.dump_as_cdf("stale.res", cdf_idx);
#endif
    check_consistency();

    // exit_lock.Unlock();

    fprintf(stdout, "master routine exit...\n");
  }
  return;
}

/* Abstract bench loader */
BenchLoader::BenchLoader(unsigned long seed) : random_generator_(seed) {
  worker_id_ = 0; /**/
}

void BenchLoader::run() { load(); }

BenchClient::BenchClient(unsigned worker_id, unsigned seed)
    : RWorker(worker_id, cm, seed) {}

void BenchClient::run() {
  ASSERT(false);
  fprintf(stdout, "[Client %d] started\n", worker_id_);
  // client only support ud msg for communication
  BindToCore(worker_id_);
#if USE_RDMA
  init_rdma();
#endif
  init_routines(coroutine_num);
  // create_server_connections(UD_MSG,nthreads);
#if CS == 1 && LOCAL_CLIENT == 0
  create_client_connections();
#endif
  running = true;
  start_routine();
  return;
}

void BenchClient::worker_routine_local(yield_func_t &yield) {

  uint64_t *start = new uint64_t[coroutine_num];
  for (uint i = 0; i < coroutine_num; ++i) {
    BenchWorker::REQ req;
    uint8_t tx_id;
    auto node = get_workload((char *)(&tx_id), random_generator[i]);
    req.c_tid = worker_id_;
    req.tx_id = tx_id;
    req.cor_id = i;
    uint thread = random_generator[i].next() % nthreads;
    start[i] = rdtsc();
    conns[thread]->enqueue(worker_id_, (char *)(&req),
                           sizeof(BenchWorker::REQ));
  }

  while (running) {
    char res[16];
    if (conns[worker_id_]->front(res)) {
      int cid = (int)res[0];
      conns[worker_id_]->pop();
      auto latency = rdtsc() - start[cid];
      timer_.emplace(latency);

      BenchWorker::REQ req;
      uint8_t tx_id;
      auto node = get_workload((char *)(&tx_id), random_generator[cid]);
      req.c_tid = worker_id_;
      req.tx_id = tx_id;
      req.cor_id = cid;
      uint thread = random_generator[cid].next() % nthreads;
      start[cid] = rdtsc();
      conns[thread]->enqueue(worker_id_, (char *)(&req),
                             sizeof(BenchWorker::REQ));
    }
    // end while
  }
}

void BenchClient::worker_routine(yield_func_t &yield) {
#if LOCAL_CLIENT
  return worker_routine_local(yield);
#endif

  char *req_buf = rpc_->get_static_buf(64);
  char reply_buf[64];
  while (true) {
    auto start = rdtsc();

    // prepare arg
    char *arg = (char *)req_buf;
    auto node = get_workload(arg + sizeof(uint8_t), random_generator[cor_id_]);
    *((uint8_t *)(arg)) = worker_id_;
    uint thread = random_generator[cor_id_].next() % nthreads;

    // send to server
    rpc_->prepare_multi_req(reply_buf, 1, cor_id_);
    rpc_->append_req(req_buf, RPC_REQ, sizeof(uint64_t), cor_id_, RRpc::REQ,
                     node, thread);

    // yield
    indirect_yield(yield);
    // get results back
    auto latency = rdtsc() - start;
    timer_.emplace(latency);
  }
}

void BenchClient::exit_handler() {
#if LOCAL_CLIENT == 0
  auto second_cycle = util::BreakdownTimer::get_one_second_cycle();
  auto m_av = timer_.report_avg() / second_cycle * 1000;
  // exit_lock.Lock();
  fprintf(stdout, "avg latency for client %d, %3f ms\n", worker_id_, m_av);
  // exit_lock.Unlock();
#endif
}

void BenchWorker::req_rpc_handler(int id, int cid, char *msg, void *arg) {
  uint8_t *input = (uint8_t *)msg;
  pending_reqs_.emplace(input[1], id, input[0], cid);
}

}; // namespace oltp

}; // namespace nocc
