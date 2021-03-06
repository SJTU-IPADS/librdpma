#pragma once

// credit: code from
// https : // github.com/NVSL/OptaneStudy/blob/master/src/kernel/memaccess.c

#define SIZEBTNT_64_AVX512                                                     \
  "vmovntdq  %%zmm0,  0x0(%%r9, %%r10) \n"                                     \
  "add $0x40, %%r10 \n"

#define SIZEBTLD_FENCE ""

namespace nvm {


void stride_nt(char *start_addr, long size, long skip, long delay, long count) {
  asm volatile(
      "xor %%r8, %%r8 \n"             /* r8: access offset */
      "xor %%r11, %%r11 \n"           /* r11: counter */
      "movq %[start_addr], %%xmm0 \n" /* zmm0: read/write register */
      // 1
      "LOOP_STRIDENT_OUTER: \n"            /* outer (counter) loop */
      "lea (%[start_addr], %%r8), %%r9 \n" /* r9: access loc */
      "xor %%r10, %%r10 \n"                /* r10: accessed size */
      "LOOP_STRIDENT_INNER: \n" /* inner (access) loop, unroll 8 times */
      SIZEBTNT_64_AVX512        /* Access: uses r10[size_accessed], r9 */
      "cmp %[accesssize], %%r10 \n"
      "jl LOOP_STRIDENT_INNER \n" SIZEBTLD_FENCE

      "xor %%r10, %%r10 \n"
      "LOOP_STRIDENT_DELAY: \n" /* delay <delay> cycles */
      "inc %%r10 \n"
      "cmp %[delay], %%r10 \n"
      "jl LOOP_STRIDENT_DELAY \n"

      "add %[skip], %%r8 \n"
      "inc %%r11 \n"
      "cmp %[count], %%r11 \n"

      "jl LOOP_STRIDENT_OUTER \n"

      ::[start_addr] "r"(start_addr),
      [accesssize] "r"(size), [count] "r"(count), [skip] "r"(skip),
      [delay] "r"(delay)
      : "%r11", "%r10", "%r9", "%r8");
}
} // namespace nvm
