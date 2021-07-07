#include <gflags/gflags.h>

#include <cmath>
#include <cpuid.h>
#include <fcntl.h>
//#include <pci/pci.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/io.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "../third_party/r2/src/logging.hh"

extern "C" {
#include "./pciutils-3.5.1/lib/pci.h"
}

#include "../third_party/rlib/core/utils/logging.hh"

/*!
  source: TODO
 */
#define PCI_AFB 0x180
#define ALL_BIT 7
#define WS_BIT 3

// llc ways
#define WAY_MASK 0xFFC00000
#define WAY_SHIFT 22
#define IIO_LLC_WAYS_REGISTER 0xC8B
int rdmsr(int msr, uint64_t *val, int cpu) {
  int fd;
  char msr_file[50];
  sprintf(msr_file, "/dev/cpu/%d/msr", cpu);

  fd = open(msr_file, O_RDONLY);
  if (fd < 0) {
    LOG(4) << "failed to open the file: " << msr_file;
    return fd;
  }

  int ret = pread(fd, val, sizeof(uint64_t), msr);
  if (ret < 0) {
    LOG(4) << "failed to read register with error: " << strerror(errno);
  }

  close(fd);

  return ret;
}

/* Write to an MSR register */
int wrmsr(int msr, uint64_t *val, int cpu) {
  int fd;
  char msr_file[50];
  sprintf(msr_file, "/dev/cpu/%d/msr", cpu);

  fd = open(msr_file, O_WRONLY);
  if (fd < 0)
    return fd;

  int ret = pwrite(fd, val, sizeof(uint64_t), msr);

  close(fd);

  return ret;
}
template <typename NAT> NAT unset_bit(const NAT &data, const int &off) {
  NAT mask = ~(1 << off);
  return data & mask;
}

bool match_dev(struct pci_dev *dev) {
  auto dca = pci_read_long(dev, PCI_AFB);
  if (dca & (1 << 6))
    return false;
  if (dca & (1 << 5))
    return false;
  if (!(dca & (1 << 4)))
    return false;
  if (dca & (1 << 2))
    return false;
  if (dca & (1 << 1))
    return false;
  return true;
}

bool check_dca_enable(struct pci_dev *dev) {
  auto dca = pci_read_long(dev, PCI_AFB);
  auto val0 = dca & (1 << ALL_BIT);
  // RDMA_LOG(2) << "check use_allocating_flow_wr: " << val;
  auto val1 = dca & (1 << WS_BIT);
  // RDMA_LOG(2) << "check nosnoopopwren " << val;
  if (val0 && !val1)
    return true;
  return false;
}

void disable_dca(struct pci_dev *dev) {
  auto dca = pci_read_long(dev, PCI_AFB);
  dca |= (1 << WS_BIT);
  dca = unset_bit(dca, ALL_BIT);
  pci_write_long(dev, PCI_AFB, dca);
}

void enable_dca(struct pci_dev *dev) {
  auto dca = pci_read_long(dev, PCI_AFB);
  dca |= (1 << ALL_BIT);
  dca = unset_bit(dca, WS_BIT);
  pci_write_long(dev, PCI_AFB, dca);
}

DEFINE_bool(modify, false, "");
DEFINE_int32(ways, 0, "");

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  RDMA_LOG(4) << "DCA setup tool started";

  bool disable = (FLAGS_ways == 0);
  bool enable = (FLAGS_ways > 1);
  int msr_cpu = 0xC8B;
  int CACHE_LEVELS = 4;
  uint32_t *ways = new uint32_t[CACHE_LEVELS];
  uint32_t eax, ebx, ecx, edx;
  for (int i = 0; i < CACHE_LEVELS; i++) {
    __get_cpuid_count(4, i, &eax, &ebx, &ecx, &edx);
    ways[i] = ((ebx & WAY_MASK) >> WAY_SHIFT) + 1;
  }
  int max_w = ways[CACHE_LEVELS - 1];

  uint64_t IIO_LLC_WAYS_VALUE, IIO_LLC_WAYS_VALUE_NEW;
  int ret = rdmsr(msr_cpu, &IIO_LLC_WAYS_VALUE, 0);
  if (ret < 0) {
    RDMA_LOG(4) << "Could not read IIO_LLC_WAYS!";
    exit(-1);
  }

  LOG(4) << "rdmsr 0x" << std::hex << IIO_LLC_WAYS_VALUE;

#if 0
  if (FLAGS_ways == 1 || FLAGS_ways == 0) {
    RDMA_LOG(4) << "N_WAYS cannt be 1. If it is zero, use dca_force to disable. ";
    exit(-1);
  }
#endif
  if (FLAGS_ways > max_w) {
    RDMA_LOG(4) << "N_WAYS should be less than " << max_w << "\n";
    exit(-1);
  }

  // FIXME: currently we assume our server has 40 virtual cpus
  for (uint i = 0;i < 40;++i) {
    u64 temp;
    int ret = rdmsr(msr_cpu, &temp, i);
    if (ret < 0) {
      RDMA_LOG(4) << "Could not read IIO_LLC_WAYS! at CPU: " << i;
      exit(-1);
    }
#if 0
    ASSERT(temp == IIO_LLC_WAYS_VALUE) << "all the cpu must be set equal: 0x"
                                       << std::hex << temp << "; 0x"
                                       << std::hex << IIO_LLC_WAYS_VALUE;
#endif

    if (FLAGS_modify) {
      // then modify ...
      IIO_LLC_WAYS_VALUE_NEW =
          (((uint64_t)(pow(2, FLAGS_ways) - 1)) << (max_w - FLAGS_ways));

      ret = wrmsr(msr_cpu, &IIO_LLC_WAYS_VALUE_NEW, i);
      if (ret < 0) {
        RDMA_LOG(2) << "Could not write IIO_LLC_WAYS";
        exit(-1);
      }
    }
  }

  LOG(4) << "Exit ...";
  return 0;


  struct pci_access *pacc;
  struct pci_dev *dev;
  u8 type;

  pacc = pci_alloc();
  pci_init(pacc);

  pci_scan_bus(pacc);
  for (dev = pacc->devices; dev; dev = dev->next) {
    pci_fill_info(dev, PCI_FILL_IDENT | PCI_FILL_BASES);

    if (dev->vendor_id == PCI_VENDOR_ID_INTEL) {

      type = pci_read_byte(dev, PCI_HEADER_TYPE);
      if (match_dev(dev)) {
        RDMA_LOG(2) << "match one";
        if (FLAGS_modify && disable) {
            RDMA_LOG(2) << "try modify to disable";
            if (check_dca_enable(dev)) {
                RDMA_LOG(2) << "and dca is now enabled";
                disable_dca(dev);
                RDMA_LOG(2) << "dca is modified from enable to disable";
            }
        } else if (FLAGS_modify && enable){
          RDMA_LOG(2) << "try modify to enable";
          if (!check_dca_enable(dev)) {
            RDMA_LOG(2) << "and dca disabled";
            enable_dca(dev);
          }
          IIO_LLC_WAYS_VALUE_NEW = (((uint64_t)(pow(2, FLAGS_ways) - 1)) << (max_w - FLAGS_ways));
          ret = wrmsr(IIO_LLC_WAYS_REGISTER, &IIO_LLC_WAYS_VALUE_NEW, msr_cpu);
          if (ret < 0) {
              RDMA_LOG(2) << "Could not write IIO_LLC_WAYS";
              exit(-1);
          }
          RDMA_LOG(2) << ("modify IIO_LLC_WAYS from 0x%x to 0x%x",
                  IIO_LLC_WAYS_VALUE, IIO_LLC_WAYS_VALUE_NEW);
        }
      }
    }
  }

  delete []ways;
    return 0;
}
