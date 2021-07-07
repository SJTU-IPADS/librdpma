#include <gflags/gflags.h>

#include <fcntl.h>
//#include <pci/pci.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/io.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

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
DEFINE_bool(enable, false, "");
DEFINE_bool(disable, false, "");

int main(int argc, char **argv) {

  gflags::ParseCommandLineFlags(&argc, &argv, true);
  RDMA_LOG(4) << "DCA setup tool started";

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
        if (check_dca_enable(dev)) {
          RDMA_LOG(2) << "and dca enabled";
          if (FLAGS_modify && FLAGS_disable) {
            RDMA_LOG(2) << "try modify to disable";
            disable_dca(dev);
            //
          }
        } else {
          RDMA_LOG(2) << "and dca disabled";
          if (FLAGS_modify && FLAGS_enable) {
            RDMA_LOG(2) << "try modify to enable";
            enable_dca(dev);
          }
        }
      }
    }
  }

    return 0;
}
