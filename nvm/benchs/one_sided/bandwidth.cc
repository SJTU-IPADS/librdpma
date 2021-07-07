#include <gflags/gflags.h>

#include "r2/src/rdma/sop.hh"
#include "rlib/core/lib.hh"

#include "../../huge_region.hh"
using namespace nvm;

DEFINE_uint32(payload, 2, "The sequential write payload (in MB)");
DEFINE_uint32(coros, 4, "#coroutines used");
DEFINE_string(addr, "localhost:8888", "Server address to connect to.");
DEFINE_uint32(total_space, 1, "Total space (in KB");

using namespace rdmaio;
using namespace rdmaio::rmem;
using namespace rdmaio::qp;

using namespace r2;
using namespace r2::rdma;

struct FastRDMA {

  DoorbellHelper<64> doorbell;
  usize start_idx = 0;
  usize cur_idx = 0;
  usize pending = 0;
  usize sent = 0;

  FastRDMA() : doorbell(IBV_WR_RDMA_WRITE) {}
  ~FastRDMA() = default;

  void write(Arc<RC> &qp, char *local_buf, const u64 &addr, const usize &sz,
             RegAttr &remote_mr, RegAttr &local_mr, RegAttr &dram_mr) {
    if (cur_idx >= 64) {
      cur_idx = 0;
    }

    doorbell.get_wr_ptr(cur_idx)->opcode = IBV_WR_RDMA_WRITE;
    doorbell.get_wr_ptr(cur_idx)->send_flags =
        (sent == 0) ? IBV_SEND_SIGNALED : 0;
    doorbell.get_wr_ptr(cur_idx)->wr.rdma.remote_addr = addr + remote_mr.buf;
    doorbell.get_wr_ptr(cur_idx)->wr.rdma.rkey = remote_mr.key;

    *doorbell.get_sge_ptr(cur_idx) = {
        .addr = (u64)(local_buf + addr), .length = sz, .lkey = local_mr.key};

    pending += 1;

    if (pending >= 16) {
      // LOG(4) << "flush w pending: " << pending << " at: " << start_idx << " "
      // << r_idx; ASSERT(cur_idx >= start_idx) << " we currently not handle
      // overflow, but it should be trivial.";
      // send
      doorbell.freeze_at(cur_idx);
      struct ibv_send_wr *bad_sr = nullptr;
      auto rc = ibv_post_send(qp->qp, doorbell.get_wr_ptr(start_idx), &bad_sr);
      if (unlikely(rc != 0)) {
        ASSERT(false) << "error rc: " << rc << strerror(errno);
      }
      qp->out_signaled = 1;
      doorbell.freeze_done_at(cur_idx);

      // reset
      pending = 0;
      start_idx = cur_idx;
    }
    cur_idx += 1;
    sent += 1;

    if (sent >= 32) {
      auto ret = qp->wait_rc_comp(1000000);
      // LOG(4) << "wait completion";
      ASSERT(ret == IOCode::Ok)
          << " poll comp error: " << RC::wc_status(std::get<1>(ret.desc)) << " "
          << ret.code.name();
      qp->out_signaled = 0;
      sent = 0;
    }
  }
};

FastRDMA fop;

void segmented_write(Arc<RC> &qp, char *local_buf, const u64 &remote_addr,
                     RegAttr &remote_mr, RegAttr &local_mr, RegAttr &dram_mr) {

  qp->bind_remote_mr(dram_mr);

  DoorbellHelper<64> doorbell(IBV_WR_RDMA_WRITE);

  usize cur_write = 0;
  u64 M = 1024 * 1024;
  u64 seq_payload = FLAGS_payload * M;

  u64 write_off = 0;

  usize start_idx = 0;
  usize cur_idx = 0;
  usize pending = 0;

  usize w_idx = 0;
  usize r_idx = 0;
  usize sent = 0;

  while (write_off < seq_payload) {

    if (cur_idx >= 64) {
      cur_idx = 0;
    }
    w_idx = cur_idx % 64;
    r_idx = w_idx + 1;
    ASSERT(r_idx < 64);

    doorbell.get_wr_ptr(w_idx)->opcode = IBV_WR_RDMA_WRITE;
    doorbell.get_wr_ptr(w_idx)->send_flags =
        (sent == 0) ? IBV_SEND_SIGNALED : 0;
    doorbell.get_wr_ptr(w_idx)->wr.rdma.remote_addr =
        remote_addr + write_off + remote_mr.buf;
    doorbell.get_wr_ptr(w_idx)->wr.rdma.rkey = remote_mr.key;

    *doorbell.get_sge_ptr(w_idx) = {.addr = (u64)(local_buf + write_off),
                                    .length = 256,
                                    .lkey = local_mr.key};

    // add a read for flush
    doorbell.get_wr_ptr(r_idx)->opcode = IBV_WR_RDMA_READ;
    doorbell.get_wr_ptr(r_idx)->send_flags = 0;
    doorbell.get_wr_ptr(r_idx)->wr.rdma.remote_addr =
        remote_addr + write_off + dram_mr.buf;
    doorbell.get_wr_ptr(r_idx)->wr.rdma.rkey = dram_mr.key;

    *doorbell.get_sge_ptr(r_idx) = {
        .addr = (u64)(local_buf), .length = 1, .lkey = local_mr.key};

    cur_idx += 2;
    if (cur_idx >= 64) {
      cur_idx = 0;
    }

    pending += 2;
    sent += 2;

    write_off += 256;
    ASSERT(write_off <= seq_payload) << write_off << " " << seq_payload;

    if (pending >= 16) {
      // LOG(4) << "flush w pending: " << pending << " at: " << start_idx << " "
      // << r_idx; ASSERT(cur_idx >= start_idx) << " we currently not handle
      // overflow, but it should be trivial.";
      // send
      doorbell.freeze_at(r_idx);
      struct ibv_send_wr *bad_sr = nullptr;
      auto rc = ibv_post_send(qp->qp, doorbell.get_wr_ptr(start_idx), &bad_sr);
      if (unlikely(rc != 0)) {
        ASSERT(false) << "error rc: " << rc << strerror(errno);
      }
      qp->out_signaled = 1;
      doorbell.freeze_done_at(r_idx);

      // reset
      pending = 0;
      start_idx = cur_idx;
    }

    // poll

    if (sent >= 32) {
      auto ret = qp->wait_rc_comp(1000000);
      // LOG(4) << "wait completion";
      ASSERT(ret == IOCode::Ok)
          << " poll comp error: " << RC::wc_status(std::get<1>(ret.desc)) << " "
          << ret.code.name();
      qp->out_signaled = 0;
      sent = 0;
    }
  }
  // flush the remaining
  if (pending > 0) {
    doorbell.freeze_at(r_idx);
    struct ibv_send_wr *bad_sr = nullptr;
    auto rc = ibv_post_send(qp->qp, doorbell.get_wr_ptr(start_idx), &bad_sr);
    if (unlikely(rc != 0)) {
      ASSERT(false) << "error rc: " << rc << strerror(errno);
    }

    qp->out_signaled += pending;
    doorbell.freeze_done_at(r_idx);

    // reset
    pending = 0;
    start_idx = cur_idx;
  }

  if (qp->out_signaled > 0) {
    auto ret = qp->wait_rc_comp(1000000);
    LOG(4) << "wait completion";
    ASSERT(ret == IOCode::Ok);
    qp->out_signaled = 0;
    sent = 0;
  }
  // LOG(4) << "done one";
#if 0
  // add a dummy read to the QP for ending
  SROp op;

  // op.set_payload(local_mem, FLAGS_payload * 1024 * 1024).set_remote_addr(off
  // % (1024 * 1024 * 1024L * 10));
  op.set_payload(local_buf, static_cast<u32>(1))
      .set_remote_addr(write_off);
  op.set_read();
  auto res = op.execute_sync(qp, IBV_SEND_SIGNALED);
  ASSERT(res == IOCode::Ok);
  qp->out_signaled = 0;
#endif
  return;
}

// a write version which directly uses RDMA's one-sided WRITE
void opt_write() { DoorbellHelper<16> doorbell(IBV_WR_RDMA_WRITE); }

void normal_write(Arc<RC> qp, char *local_mem, RegAttr &remote_mr,
                  RegAttr &local_mr, RegAttr &dram_mr) {
  LOG(4) << "in normal write evaluting!, using " << FLAGS_coros << " coroutines.";
  usize write_cnt = 0;

  SScheduler ssched;

  usize total = 0;
  r2::Timer t;
  u64 off = 0;

  u64 M = 1024 * 1024;
  u64 seq_payload = FLAGS_payload * M;
  // u64 total_space = 1024 * 1024 * 1024L * 10;
  u64 total_space = FLAGS_total_space * 1024L;

  seq_payload = FLAGS_payload;

  for (uint i = 0; i < FLAGS_coros; ++i) {
    ssched.spawn([qp,local_mem, remote_mr, local_mr, dram_mr, &total, &t, &off, &write_cnt,seq_payload,total_space](R2_ASYNC) {

      while (1) {
        write_cnt += 1;
        if (write_cnt * seq_payload >= (100L * 1024 * 1024 * 1024)) {
          // return;
        }

        // do the write
#if 1
        SROp op;

        // op.set_payload(local_mem, FLAGS_payload * 1024 *
        // 1024).set_remote_addr(off % (1024 * 1024 * 1024L * 10));
        ASSERT(off % 256 == 0);
        op.set_payload(local_mem, static_cast<u32>(seq_payload))
            .set_remote_addr(off % total_space);
        op.set_write();
        // op.set_read();
        //auto res = op.execute_sync(qp, IBV_SEND_SIGNALED);
        auto res =
            op.execute(qp, IBV_SEND_SIGNALED, R2_ASYNC_WAIT);

        ASSERT(res == IOCode::Ok);
#else
        // LOG(4) << "start write one: "; sleep(1);
        // segmented_write(qp, local_mem, off % total_space, remote_mr,
        // local_mr,dram_mr);
        auto addr = off % total_space;
        if (addr == total_space) {
          addr = 0;
        }
        fop.write(qp, local_mem, addr, seq_payload, remote_mr, local_mr,
                  dram_mr);
#endif
        off += seq_payload;

        total += 1;
        if (total > 100 && t.passed_msec() >= 1000000) {
          // print thpt
          double msec = t.passed_msec();
          double thpt = total / msec * 1000000;
          double bandwidth = (thpt * FLAGS_payload) / (1024L * 1024 * 1024) * 8;
          LOG(4) << "thpt: " << thpt << " reqs/sec"
                 << " ;bandwidth: " << bandwidth << " Gbps";
          t.reset();
          total = 0;
        }
      }
    });
  }
  ssched.run();
}

int main(int argc, char **argv) {

  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // no need to bind the nic
  auto idx = 0;

  auto nic = RNic::create(RNicInfo::query_dev_names().at(idx)).value();
  auto qp = RC::create(nic, QPConfig()).value();

  // setup RDMA
  ConnectManager cm(FLAGS_addr);
  // RDMA_LOG(2) << "start to connect to server: " << FLAGS_addr;
  auto wait_res = cm.wait_ready(1000000, 4);
  if (wait_res ==
      IOCode::Timeout) // wait 1 second for server to ready, retry 2 times
    RDMA_ASSERT(false) << "cm connect to server timeout " << wait_res.desc;
  char qp_name[64];
  snprintf(qp_name, 64, "%rc:%d:@%d", 0, 73);
  u64 key = 0;

  int rnic_idx = idx;
  // int rnic_idx = 0;
  auto qp_res = cm.cc_rc(qp_name, qp, rnic_idx, QPConfig());
  RDMA_ASSERT(qp_res == IOCode::Ok) << std::get<0>(qp_res.desc);
  RDMA_LOG(4) << "qp connected done!";

  auto mem_sz = 1024 * 1024 * 16;
  RDMA_LOG(4) << "use mem sz: " << FLAGS_payload << " MB";

  // use huge page to for local RDMA buffer
  auto huge_region = HugeRegion::create(mem_sz).value();
  auto local_mem = std::make_shared<RMem>( // nvm_region->size(),
      huge_region->size(),
      [&huge_region](u64 s) -> RMem::raw_ptr_t {
        return huge_region->start_ptr();
      },
      [](RMem::raw_ptr_t ptr) {
        // currently we donot free the resource, since the region will use
        // to the end
      });

  auto local_mr = RegHandler::create(local_mem, nic).value();

  auto fetch_res = cm.fetch_remote_mr(rnic_idx);
  RDMA_ASSERT(fetch_res == IOCode::Ok) << std::get<0>(fetch_res.desc);
  auto remote_attr = std::get<1>(fetch_res.desc);
  qp->bind_remote_mr(remote_attr);

#if 1
  fetch_res = cm.fetch_remote_mr(rnic_idx * 73 + 73);
  RDMA_ASSERT(fetch_res == IOCode::Ok) << std::get<0>(fetch_res.desc);
  auto dram_mr = std::get<1>(fetch_res.desc);
#endif

  RDMA_LOG(4) << "MR init done";
  auto local_attr = local_mr->get_reg_attr().value();
  qp->bind_local_mr(local_attr);

  normal_write(qp, (char *)huge_region->start_ptr(), remote_attr, local_attr,
               dram_mr);

  return 0;
}
