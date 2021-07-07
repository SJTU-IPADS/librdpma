#pragma once

#include "rlib/tests/random.hh"

#include "r2/src/common.hh"

namespace nvm {

class RandomAddr {
  const ::r2::u64 space = 0;
  const ::r2::u64 off = 0;

public:
  explicit RandomAddr(const ::r2::u64 &space, const ::r2::u64 off)
      : space(space), off(off) {}
  ::r2::u64 gen(::test::FastRandom &rand) {
    return (off + rand.next() % space) + off;
  }
};

class SeqAddr {
public:
  const ::r2::u64 space = 0;
  const ::r2::u64 off = 0;
  ::r2::u64 cur_off = 0;

  explicit SeqAddr(const ::r2::u64 &space, const ::r2::u64 off) : space(space), off(off) {}

  ::r2::u64 gen(const ::r2::u64 &step) {
    auto res = off + cur_off % space;
    cur_off += step;
    return res;
  }
};

} // namespace nvm
