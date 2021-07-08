#pragma once

#include "core/rdma_sched.h"
#include "rdmaio.h"

#include <emmintrin.h>
#include <immintrin.h>
#include <x86intrin.h>
#include <xmmintrin.h>

//#include "../../nvm/benchs/two_sided/core.hh"

#include "../../nvm/nvm_region.hh"

using namespace nvm;

extern ::rdmaio::Arc<MemoryRegion> nvm_region;

namespace nocc {

namespace rtx {

using namespace rdmaio;
using namespace oltp;

inline usize nt_memcpy(char *src, u64 size, char *target) {
  auto cur_ptr = src;
  usize cur = 0;

  for (cur = 0; cur < size;) {
    // in 64 bytes granulaity
    for (uint i = 0; i < 8; ++i) {
      // I believe that compiler will unroll this
      _mm_stream_pi((__m64 *)(target + cur), *((__m64 *)(src + cur)));
      cur += sizeof(u64);
    }
  }
  return cur;
}

class RpcLogger : public Logger {
public:
  RpcLogger(RRpc *rpc, int log_rpc_id, int ack_rpc_id, uint64_t base_off,
            int expected_store, char *local_p, int ms, int ts, int size,
            int entry_size = RTX_LOG_ENTRY_SIZE)
      : Logger(rpc, ack_rpc_id, base_off, expected_store, local_p, ms, ts, size,
               entry_size),
        log_rpc_id_(log_rpc_id) {
    // register RPC if necessary
    rpc->register_callback(
        std::bind(&RpcLogger::log_remote_handler, this, std::placeholders::_1,
                  std::placeholders::_2, std::placeholders::_3,
                  std::placeholders::_4),
        log_rpc_id_, true);
  }

  inline void log_remote(BatchOpCtrlBlock &clk, int cor_id) {
    assert(clk.batch_size_ > 0);
    clk.send_batch_op(rpc_handler_, cor_id, log_rpc_id_, false);
  }

  void log_remote_handler(int id, int cid, char *msg, void *arg) {
    int size = (uint64_t)arg;
    char *local_ptr = mem_.get_next_log(id,rpc_handler_->worker_id_,size);
    //int thread_sz = 4096 * 16;
    //char *local_ptr = (char *)(nvm_region->addr) + rpc_handler_->worker_id_ * thread_sz + 4096 * cid;
    assert(size < RTX_LOG_ENTRY_SIZE && size > 0);
    // memcpy(local_ptr,msg,size);
    //memcpy(local_buffer + cid * MAX_MSG_SIZE, msg, size);
    //assert(nt_memcpy(msg,size,local_buffer + cid * MAX_MSG_SIZE) >= size);
    assert(nt_memcpy(msg,size,local_ptr) >= size);
    asm volatile("sfence" : : : "memory");
    //LOG(4) << "size: " << size ; sleep(1);
    //LOG(4) << "write to nvm_region: " << (void *)local_ptr << ", start of the region: "
    //<< nvm_region->addr; sleep(1);

    char *reply_msg = rpc_handler_->get_reply_buf();
    rpc_handler_->send_reply(reply_msg, 0, id, cid); // a dummy reply
  }

private:
  const int log_rpc_id_;
  char local_buffer[MAX_MSG_SIZE * 32];
};

} // namespace rtx
} // namespace nocc
