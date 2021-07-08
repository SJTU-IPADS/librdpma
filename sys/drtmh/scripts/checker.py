#!/usr/bin/env python

from batch_bench_runner import parse_hosts
from run_util import print_with_tag
from subprocess import Popen, PIPE

import sys

def check_hugepage():
    mac_set = parse_hosts()
    for m in mac_set:
        bcmd = "cat  /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages"
        stdout, stderr = Popen(['ssh',"-o","ConnectTimeout=1",m, bcmd],
                               stdout=PIPE).communicate()
        huge_page_num = int(stdout)
        if (huge_page_num < 8192):
            print_with_tag("check","mac %s huge page %f failed" % (m,huge_page_num))

    return

def check_status():
    mac_set = parse_hosts()
    for m in mac_set:
        bcmd = "ps aux | grep nocc"
        print("check " + m)
        stdout, stderr = Popen(['ssh',"-o","ConnectTimeout=1",m, bcmd],
                               stdout=PIPE).communicate()
        print(stdout)
    return

def check_log():
    mac_set = parse_hosts()
    for m in mac_set:
        bcmd = "cat log"
        print("check " + m)
        stdout, stderr = Popen(['ssh',"-o","ConnectTimeout=1",m, bcmd],
                               stdout=PIPE).communicate()
        print(stdout)
    return


def main():
    code = 0
    if len(sys.argv) > 1:
        code = int(sys.argv[1])
    if code == 0:
        check_hugepage()
    if code == 1:
        check_status()
    if code == 2:
        check_log()

    return


if __name__ == "__main__":
    main()
