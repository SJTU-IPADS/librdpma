#ifndef __USE_FILE_OFFSET64
#define __USE_FILE_OFFSET64
#endif
#define TEST_NRFS_IO
//#define TEST_RAW_IO
#include "mpi.h"
#include "nrfs.h"
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#define BUFFER_SIZE 0x1000000

#include "../fs/huge_region.hh"
using namespace nvm;

#include "r2/src/timer.hh"
#include "r2/src/common.hh"
#include "r2/src/random.hh"
using namespace r2;

#include "rlib/core/lib.hh"
using namespace rdmaio;
using namespace rdmaio::rmem;
using namespace rdmaio::qp;

extern Arc<RC> nvm_qp;
extern char *local_ptr;

int myid, file_seq;
int numprocs;
nrfs fs;
char buf[BUFFER_SIZE];
int mask = 0;
int collect_time(int cost) {
  int i;
  char message[8];
  MPI_Status status;
  int *p = (int *)message;
  int max = cost;
  for (i = 1; i < numprocs; i++) {
    MPI_Recv(message, 8, MPI_CHAR, i, 99, MPI_COMM_WORLD, &status);
    if (*p > max)
      max = *p;
  }
  return max;
}

void write_test(int size, int op_time) {
  char path[255];
  int i;
  double start, end, rate, num;
  int time_cost;
  char message[8];
  int *p = (int *)message;

  /* file open */
  sprintf(path, "/file_%d", file_seq);
  nrfsOpenFile(fs, path, O_CREAT);
  printf("create file: %s\n", path);
  memset(buf, 'a', BUFFER_SIZE);

  MPI_Barrier(MPI_COMM_WORLD);

  start = MPI_Wtime();
  r2::Timer t;
  r2::util::FastRandom rand(0xdeadbeaf + file_seq);

  for (i = 0; i < op_time; i++) {
    auto off = rand.next() % 4096 * 1024 * 1024;
#ifdef TEST_RAW_IO
    // XD: the execution path
    nrfsRawWrite(fs, path, buf, size, 0);
#endif
#ifdef TEST_NRFS_IO
    nrfsWrite(fs, path, buf, size, 0,off);
#endif
  }
  end = MPI_Wtime();

  MPI_Barrier(MPI_COMM_WORLD);

  *p = (int)((end - start) * 1000000);

  if (myid != 0) {
    MPI_Send(message, 8, MPI_CHAR, 0, 99, MPI_COMM_WORLD);
  } else {
    time_cost = collect_time(*p);
    num = (double)(size * op_time * numprocs) / time_cost;
    rate = 1000000 * num / 1024 / 1024;
    auto thpt = rate * (1024 * 1024) / (size);

    printf("Write Bandwidth = %f MB/s TimeCost = %d; thpt: %f K\n",
           rate, (int)time_cost, thpt / 1000.0);
  }

  {
    // calculate my thpt
    //auto time_cost = (int)((end - start) * 1000000);
    auto time = t.passed_msec();
    ASSERT(time > 0);
    double num = (double)(size * (double)op_time) / time;
    double rate = 1000000 * num / 1024 / 1024;
    ASSERT(rate > 0);
    auto thpt = rate * (1024 * 1024) / (size);
    printf("My write Bandwidth = %f MB/s TimeCost = %d; thpt: %f K\n", rate,
           (int)time, thpt / 1000.0);
  }

  nrfsCloseFile(fs, path);

  file_seq += 1;
  if (file_seq == numprocs)
    file_seq = 0;
}

void read_test(int size, int op_time) {
  char path[255];
  int i;
  double start, end, rate, num;
  int time_cost;
  char message[8];
  int *p = (int *)message;

  memset(buf, '\0', BUFFER_SIZE);
  memset(path, '\0', 255);
  sprintf(path, "/file_%d", file_seq);

  MPI_Barrier(MPI_COMM_WORLD);

  start = MPI_Wtime();
  for (i = 0; i < op_time; i++) {
#ifdef TEST_RAW_IO
    nrfsRawRead(fs, path, buf, size, 0);
#endif
#ifdef TEST_NRFS_IO
    nrfsRead(fs, path, buf, size, 0);
#endif
  }
  end = MPI_Wtime();

  MPI_Barrier(MPI_COMM_WORLD);

  *p = (int)((end - start) * 1000000);

  if (myid != 0) {
    MPI_Send(message, 8, MPI_CHAR, 0, 99, MPI_COMM_WORLD);
  } else {
    time_cost = collect_time(*p);
    num = (double)(size * op_time * numprocs) / time_cost;
    rate = 1000000 * num / 1024 / 1024;
    printf("Read Bandwidth = %f MB/s TimeCost = %d\n", rate, (int)time_cost);
  }
  MPI_Barrier(MPI_COMM_WORLD);

  file_seq += 1;
  if (file_seq == myid)
    file_seq = 0;
}

int main(int argc, char **argv) {

  char path[255];
  if (argc < 3) {
    fprintf(stderr, "Usage: ./mpibw block_size\n");
    return -1;
  }
  int block_size = atoi(argv[1]);
  int op_time = atoi(argv[2]);
  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &myid);
  MPI_Comm_size(MPI_COMM_WORLD, &numprocs);

  // LOG(4) << "num procs: " << numprocs;
  file_seq = myid;
  MPI_Barrier(MPI_COMM_WORLD);

  /* nrfs connection */
  fs = nrfsConnect("default", 0, 0);

  MPI_Barrier(MPI_COMM_WORLD);
  // LOG(4) << "block sz: " << block_size << "; op_time: " << op_time;

  auto huge_region = std::make_shared<DRAMRegion>(2 * 1024 * block_size);
  local_ptr = (char *)huge_region->addr;
  // RLib setup
  auto local_mem = std::make_shared<RMem>( // nvm_region->size(),
      huge_region->size(),
      [&huge_region](u64 s) -> RMem::raw_ptr_t {
        return huge_region->start_ptr();
      },
      [](RMem::raw_ptr_t ptr) {
        // currently we donot free the resource,
        // since the region will use to the end
      });

  auto nic = RNic::create(RNicInfo::query_dev_names().at(0)).value();
  nvm_qp = RC::create(nic, QPConfig()).value();

  // 2. create the pair QP at server using CM
  ConnectManager cm("r740:6666");
  // RDMA_LOG(2) << "start to connect to server: " << FLAGS_addr;
  auto wait_res = cm.wait_ready(2000000, 12);
  if (wait_res ==
      IOCode::Timeout) // wait 1 second for server to ready, retry 2 times
    RDMA_ASSERT(false) << "cm connect to server timeout " << wait_res.desc;

  char qp_name[64];
  snprintf(qp_name, 64, "%rc:%d", myid);
  u64 key = 0;

  int rnic_idx = 0;

  auto qp_res = cm.cc_rc(qp_name, nvm_qp, rnic_idx, QPConfig());
  RDMA_ASSERT(qp_res == IOCode::Ok) << std::get<0>(qp_res.desc);
  // RDMA_LOG(4) << "client fetch QP authentical key: " << key;

  // 3. create the local MR for usage, and create the remote MR for usage

  auto local_mr = RegHandler::create(local_mem, nic).value();

  auto fetch_res = cm.fetch_remote_mr(rnic_idx);
  RDMA_ASSERT(fetch_res == IOCode::Ok) << std::get<0>(fetch_res.desc);
  auto remote_attr = std::get<1>(fetch_res.desc);

  fetch_res = cm.fetch_remote_mr(rnic_idx * 73 + 73);
  RDMA_ASSERT(fetch_res == IOCode::Ok) << std::get<0>(fetch_res.desc);
  auto dram_mr = std::get<1>(fetch_res.desc);

  // RDMA_LOG(4) << "remote attr's key: " << remote_attr.key << " "
  //<< local_mr->get_reg_attr().value().key;

  nvm_qp->bind_remote_mr(remote_attr);
  auto local_attr = local_mr->get_reg_attr().value();
  nvm_qp->bind_local_mr(local_attr);

  r2::compile_fence();

  write_test(1024 * block_size, op_time);
  // read_test(1024 * block_size, op_time);

  MPI_Barrier(MPI_COMM_WORLD);
  sprintf(path, "/file_%d", myid);
  nrfsDelete(fs, path);
  nrfsDisconnect(fs);

  MPI_Finalize();
}
