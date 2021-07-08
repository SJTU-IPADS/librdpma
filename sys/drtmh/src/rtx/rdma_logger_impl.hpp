#pragma once

#include "core/rdma_sched.h"
#include "rdmaio.h"

#include "rlib/core/lib.hh"
#include "rlib/core/qps/config.hh"
#include "rlib/core/rctrl.hh"
#include "rlib/core/qps/doorbell_helper.hh"

#include "./opt_config.hh"
#include "./rdma_req_helper.hpp"

extern thread_local ::rdmaio::Arc<::rdmaio::qp::RC> nvm_qp;
extern thread_local ::rdmaio::rmem::RegAttr nvm_mr;
extern thread_local ::rdmaio::rmem::RegAttr dram_mr;
extern thread_local ::rdmaio::rmem::RegAttr local_mr;
extern thread_local ::rdmaio::Qp *wrapper_nvm_qp;

extern thread_local ::rdmaio::Qp *wrapper_nvm_qp1;

namespace nocc {
namespace rtx {

using namespace rdmaio;
using namespace oltp;

#if 0
template <typename T>
inline constexpr T round_up(const T &num, const T &multiple) {
  assert(multiple && ((multiple & (multiple - 1)) == 0));
  return (num + multiple - 1) & -multiple;
}
#endif

class RDMALogger : public Logger {
 public:
  RDMALogger(RdmaCtrl *cm, RScheduler* rdma_sched,int nid,int tid,uint64_t base_off,
             RRpc *rpc,int ack_rpc_id,
             int expected_store,char *local_p,int ms,int ts,int size,int entry_size = RTX_LOG_ENTRY_SIZE)
      :Logger(rpc,ack_rpc_id,base_off,expected_store,local_p,ms,ts,size,entry_size),
       node_id_(nid),worker_id_(tid),
       scheduler_(rdma_sched)
  {
    // init local QP vector
    fill_qp_vec(cm,worker_id_);
  }

  inline void log_remote(BatchOpCtrlBlock &clk, int cor_id) {

    int size = clk.batch_msg_size(); // log size
#if SZ_OPT
    if (size <= 64) {
      size = 64;
    } else {
        // align to XPline (256)
        size = round_up<int>(size, 256);
    }
#endif


    assert(clk.mac_set_.size() > 0);
    //LOG(4) << "log remte sz: " << size; sleep(1);
    // post the log to remote servers
    for(auto it = clk.mac_set_.begin();it != clk.mac_set_.end();++it) {
      //int  mac_id = *it;
      int mac_id = 0; // only use 0 for logging
      //auto qp = qp_vec_[mac_id];
      auto qp = get_qp(mac_id);
      auto off = mem_.get_remote_log_offset(node_id_,worker_id_,mac_id,size);
      //LOG(4) << "off: " << off;
#if SZ_OPT
      ASSERT(off % 64 == 0);
#endif
#if !FLUSH_OPT
      assert(off != 0);
      scheduler_->post_send(qp,cor_id,
                            IBV_WR_RDMA_WRITE,clk.req_buf_,size,off,
                            IBV_SEND_SIGNALED | ((size < 64)?IBV_SEND_INLINE:0));
#else
      RDMAReqBase base(cor_id);
      base.sge[0].length = size;
      base.sge[0].addr = (uint64_t)clk.req_buf_;
      base.sr[0].wr.rdma.remote_addr = off;
      base.sr[0].opcode = IBV_WR_RDMA_WRITE;

      base.sge[1].length = sizeof(u8);
      base.sge[1].addr = (uint64_t)clk.req_buf_;
      base.sr[1].wr.rdma.remote_addr = off % (2 * 1024 * 1024);
      base.sr[1].opcode = IBV_WR_RDMA_READ;

      qp = wrapper_nvm_qp1;

#if 1
      base.post_reqs_impl_1(scheduler_,wrapper_nvm_qp,qp);
#else
      base.post_reqs_impl(scheduler_,qp);
#endif

#endif
      break;
    }
    // requires yield call after this!
  }

 private:
  RScheduler *scheduler_;
  int node_id_;
  int worker_id_;

#include "qp_selection_helper.h"
};

}; // namespace rtx
}; // namespace nocc
