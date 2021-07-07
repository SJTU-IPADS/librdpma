
#pragma once

#include "r2/src/common.hh"

namespace nvm {

/*!
  Only record the *average* latency.
 */
struct FlatLatRecorder {
  double cur_lat = 0.0;
  ::r2::u64 counts = 1;

  FlatLatRecorder() = default;

  double get_lat() const { return cur_lat; }

  void add_one(double lat) {
    cur_lat += (lat - cur_lat) / counts;
    counts += 1;
  }
};

} // namespace nvm
