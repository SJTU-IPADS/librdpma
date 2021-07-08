#include <emmintrin.h>
#include <immintrin.h>
#include <x86intrin.h>
#include <xmmintrin.h>

#include "tx_config.h"
#include "./opt_config.hh"

//#include "../../third_party/r2/src/common.hh"

//#include "../../nvm/benchs/two_sided/core.hh"

#include <utility> // for forward

namespace nocc {

namespace rtx  {

  //using namespace r2;

#define CONFLICT_WRITE_FLAG 73

  // implementations of TX's get operators
  inline __attribute__((always_inline)) MemNode *
  TXOpBase::local_lookup_op(int tableid, uint64_t key) {
    MemNode *node = db_->stores_[tableid]->Get(key);
    return node;
}

inline __attribute__((always_inline))
MemNode *TXOpBase::local_get_op(MemNode *node,char *val,uint64_t &seq,int len,int meta) {
retry: // retry if there is a concurrent writer
  char *cur_val = (char *)(node->value);
  seq = node->seq;
  asm volatile("" ::: "memory");
#if INLINE_OVERWRITE
  memcpy(val,node->padding + meta,len);
#else
  memcpy(val,cur_val + meta,len);
#endif
  asm volatile("" ::: "memory");
  if( unlikely(node->seq != seq || seq == CONFLICT_WRITE_FLAG) ) {
    goto retry;
  }
  return node;
}

inline __attribute__((always_inline))
MemNode * TXOpBase::local_get_op(int tableid,uint64_t key,char *val,int len,uint64_t &seq,int meta) {
  MemNode *node = local_lookup_op(tableid,key);
  assert(node != NULL);
  assert(node->value != NULL);
  return local_get_op(node,val,seq,len,meta);
}

inline __attribute__((always_inline))
MemNode *TXOpBase::local_insert_op(int tableid,uint64_t key,uint64_t &seq) {
  MemNode *node = db_->stores_[tableid]->GetWithInsert(key);
  assert(node != NULL);
  seq = node->seq;
  return node;
}

inline __attribute__((always_inline))
bool TXOpBase::local_try_lock_op(MemNode *node,uint64_t lock_content) {
  assert(lock_content != 0); // 0: not locked
  volatile uint64_t *lockptr = &(node->lock);
  if( unlikely( (*lockptr != 0) ||
                !__sync_bool_compare_and_swap(lockptr,0,lock_content)))
    return false;
  return true;
}

inline __attribute__((always_inline))
MemNode *TXOpBase::local_try_lock_op(int tableid,uint64_t key,uint64_t lock_content) {

  MemNode *node = db_->stores_[tableid]->Get(key);
  assert(node != NULL && node->value != NULL);
  if(local_try_lock_op(node,lock_content))
     return node;
  return NULL;
}

inline __attribute__((always_inline))
bool TXOpBase::local_try_release_op(MemNode *node,uint64_t lock_content) {
  volatile uint64_t *lockptr = &(node->lock);
  return __sync_bool_compare_and_swap(lockptr,lock_content,0);
}

inline __attribute__((always_inline))
bool TXOpBase::local_try_release_op(int tableid,uint64_t key,uint64_t lock_content) {
  MemNode *node = db_->stores_[tableid]->GetWithInsert(key);
  return local_try_release_op(node,lock_content);
}

inline __attribute__((always_inline))
bool TXOpBase::local_validate_op(MemNode *node,uint64_t seq) {
  return (seq == node->seq) && (node->lock == 0);
}

inline __attribute__((always_inline))
bool TXOpBase::local_validate_op(int tableid,uint64_t key,uint64_t seq) {
  MemNode *node = db_->stores_[tableid]->Get(key);
  return local_validate_op(node,seq);
}

template <typename T>
inline constexpr T round_up(const T &num, const T &multiple) {
  assert(multiple && ((multiple & (multiple - 1)) == 0));
  return (num + multiple - 1) & -multiple;
}

inline int nt_memcpy_128(char *src, int size, char *target) {
  uint64_t src_p = (uint64_t)src;
  src_p = round_up<uint64_t>(src_p, 64);

  uint64_t target_p = (uint64_t)target;
  target_p = round_up<uint64_t>(target_p,64);

  auto cur_ptr = (char *)src_p;
  int cur = 0;
#if OPT_NT_ALIGN
  // LOG(4) << "in copy"; sleep(1);
  ASSERT((uint64_t)src_p % 32 == 0);
  ASSERT((uint64_t)target_p % 32 == 0);

  for (cur = 0; cur < size;) {
    cur += sizeof(__m128i);
    //_mm_stream_si128((__m128i *)(target + cur), *((__m128i *)(src + cur)));
    _mm_stream_si128((__m128i *)(target_p + cur), *((__m128i *)(src_p + cur)));
    // LOG(4) << "copied: " << cur;
  }
#else
  for (cur = 0; cur < size;) {
    cur += sizeof(__m64);
    _mm_stream_pi((__m64 *)(target + cur), *((__m64 *)(src + cur)));
    // LOG(4) << "copied: " << cur;
  }
#endif
  // LOG(4) << "copy done: " << cur;
  return cur;
}

inline __attribute__((always_inline))
MemNode *TXOpBase::inplace_write_op(MemNode *node,char *val,int len,int meta) {
#if OPT_NT && 1
  //ASSERT(false);
  nt_memcpy_128(val, len, (char *)node->value);
#else
  auto old_seq = node->seq;assert(node->seq != 1);
#if !DRAM_LOCK
  node->seq = CONFLICT_WRITE_FLAG;
  asm volatile("" ::: "memory");
#endif
#if INLINE_OVERWRITE
  memcpy(node->padding,val,len);
#else
  if(node->value == NULL) {
    node->value = (uint64_t *)malloc(len);
  }
  memcpy((char *)(node->value) + meta,val,len);
#endif
#if !DRAM_LOCK
  // release the locks
  asm volatile("" ::: "memory");
  node->seq = old_seq + 2;
  asm volatile("" ::: "memory");
  node->lock = 0;
#endif
#endif
  return node;
}

inline __attribute__((always_inline))
MemNode *TXOpBase::inplace_write_op(int tableid,uint64_t key,char *val,int len) {
  MemNode *node = db_->stores_[tableid]->Get(key);
  ASSERT(node != NULL) << "get node error, at [tab " << tableid
                       << "], key: "<< key;
  return inplace_write_op(node,val,len,db_->_schemas[tableid].meta_len);
}


template <typename REQ,typename... _Args>
inline  __attribute__((always_inline))
uint64_t TXOpBase::rpc_op(int cid,int rpc_id,int pid,
                          char *req_buf,char *res_buf,_Args&& ... args) {
  // prepare the arguments
  *((REQ *)req_buf) = REQ(std::forward<_Args>(args)...);

  // send the RPC
  rpc_->prepare_multi_req(res_buf,1,cid);
  rpc_->append_req(req_buf,rpc_id,sizeof(REQ),cid,RRpc::REQ,pid);
}



// } end class
} // namespace rtx
} // namespace nocc
