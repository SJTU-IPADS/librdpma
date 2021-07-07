#include <gflags/gflags.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <assert.h>

#include "rlib/tests/random.hh"

#include "./gen_addr.hh"

#include "two_sided/core.hh"
#include "two_sided/r740.hh"

#include "../huge_region.hh"
#include "../nvm_region.hh"

#include "./statucs.hh"
#include "./thread.hh"

#include "./nt_memcpy.hh"

DEFINE_string(nvm_file, "/dev/dax1.6", "Abstracted NVM device");
DEFINE_uint64(nvm_sz, 10, "Mapped sz (in GB), should be larger than 2MB");
DEFINE_int64(threads, 1, "Number of threads used.");
DEFINE_int64(payload, 256, "Number of bytes to write");

DEFINE_bool(clflush, true, "whether to flush write content");

DEFINE_bool(use_nvm, true, "whether to use NVM.");

DEFINE_bool(random, false, "");
DEFINE_bool(round_up, true, "");
DEFINE_uint32(round_payload, 256, "Roundup of the write payload");

using namespace nvm;
using namespace test;

template <typename Nat> Nat align(const Nat &x, const Nat &a) {
  auto r = x % a;
  return r ? (x + a - r) : x;
}

volatile bool running = true;

int NO_OPT main(int argc, char **argv) {

  gflags::ParseCommandLineFlags(&argc, &argv, true);

  LOG(4) << "Hello NVM!, using file: " << FLAGS_nvm_file;

  u64 sz = static_cast<u64>(FLAGS_nvm_sz) * (1024 * 1024 * 1024L);

  using TThread = Thread<int>;
  std::vector<std::unique_ptr<TThread>> threads;
  std::vector<Statics> statics(FLAGS_threads);

  for (uint thread_id = 0; thread_id < FLAGS_threads; ++thread_id) {
    threads.push_back(std::make_unique<TThread>([thread_id, &statics, sz]() {
      Arc<MemoryRegion> nvm_region = nullptr;

      if (FLAGS_use_nvm) {
        RDMA_LOG(4) << "server uses NVM with size: " << FLAGS_nvm_sz << " GB";
        u64 sz = static_cast<u64>(FLAGS_nvm_sz) * (1024 * 1024 * 1024L);
        nvm_region = NVMRegion::create(FLAGS_nvm_file, sz).value();
      } else {
        RDMA_LOG(4) << "server uses DRAM (huge page)";
        u64 sz = static_cast<u64>(FLAGS_nvm_sz) * (1024 * 1024 * 1024L);
        // nvm_region = std::make_shared<DRAMRegion>(FLAGS_nvm_sz);
        nvm_region = HugeRegion::create(sz).value();
      }

      // auto nvm_region = NVMRegion::create(FLAGS_nvm_file, sz).value();

      // bind_to_core(thread_id + per_socket_cores) ;
      bind_to_core(thread_id);
      char *local_buf = new char[4096 * 2];

      FastRandom rand(0xdeadbeaf + thread_id * 73);

      u64 total_sz = (sz - FLAGS_payload - 4096);
      ASSERT(total_sz < sz);

      u64 per_thread_sz = sz / FLAGS_threads;
      ASSERT(per_thread_sz >= (4096 + FLAGS_payload));
      per_thread_sz -= (4096 + FLAGS_payload);

      u64 sum = 0;
      u64 off = 0;
      // main evaluation loop

      RandomAddr rgen(sz, 0); // random generator

      const usize access_gra = 1024 * 1024;

      while (running) {
        const usize nslots = 100 * 1024;
        const int fourK = 4 * 1024;
        const usize slot_span = 6 * fourK;    // 6 DIMMs
        usize slot_num = 0;
        usize naddr = 0, addr = 0;
        if (FLAGS_random) {
            // total visiting space: 100K * 24KB = 2.4GB
            slot_num = rand.next() % nslots;
            auto base = fourK - FLAGS_payload * 2;
            ASSERT(base > 0);
            naddr = rand.next() % base;

            if (FLAGS_round_up) {
                naddr = round_up<u64>(addr, FLAGS_round_payload);
            }
            addr = slot_num * slot_span + naddr;
            ASSERT(addr % FLAGS_round_payload == 0);

        } else {
            addr = slot_num * slot_span + naddr;
            addr = round_up<u64>(addr, FLAGS_round_payload);

            naddr += FLAGS_payload;
            if (naddr + FLAGS_payload > fourK) {
                slot_num++;
                naddr = 0;
            }
        }

        if (addr + FLAGS_payload >= sz) {
          ASSERT(false);
          addr = sz - FLAGS_payload - 4096;
        }

        LOG(2) << "Addr " << addr;
        sleep(1);
        char *server_buf_ptr =
            reinterpret_cast<char *>(nvm_region->addr) + addr;
#if 0
        *((u64 *)local_buf) = 73;
        nt_memcpy(local_buf,64,server_buf_ptr);
        asm volatile("sfence" : : : "memory");

        ASSERT(*((u64 *)server_buf_ptr) == 73)
            << " server buf value: " << *((u64 *)server_buf_ptr);
#endif

        // randomly init the local buf
        //for (uint i = 0; i < FLAGS_payload; ++i) {
        //          local_buf[i] = addr + i;
        //}

#if 1
        if (FLAGS_clflush) {
          ASSERT(nt_write(local_buf, FLAGS_payload, server_buf_ptr) >=
          FLAGS_payload) << "nvm write payload: " << FLAGS_payload;
          //nvm_write(local_buf,FLAGS_payload, server_buf_ptr);
        }
        else{
          //ASSERT(memcpy_flush_write(local_buf, FLAGS_payload, server_buf_ptr)
          //>= FLAGS_payload);
          memcpy(server_buf_ptr, local_buf, FLAGS_payload);
        }
#endif
        // ASSERT(nvm_read(local_buf, FLAGS_payload, server_buf_ptr) >=
        // FLAGS_payload);
        off += FLAGS_payload;

        r2::compile_fence();
        statics[thread_id].inc(1);
      }
      LOG(4) << "total off: " << off;
      return 0;
    }));
  }

  for (auto &t : threads)
    t->start();

  LOG(2) << "all bench threads started";

  Reporter::report_thpt(statics, 60);
  running = false;

  for (auto &t : threads) {
    t->join();
  }

  sleep(1);

  return 0;
}
