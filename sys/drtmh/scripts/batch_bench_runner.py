#! /usr/bin/env python

import sys
import os
import commands
import time
import subprocess
import signal # bind interrupt handler
import xml.etree.cElementTree as ET ## parsing the xml file

from optparse import OptionParser

## self-defined imports
from runner import RoccRunner

FNULL = open(os.devnull, 'w')
r = None

def sigint_handler(sig,frame):
    global r
    if r != None:
        r.dump_results("trace-error")
    sys.exit(-1)

def parse_hosts(config="hosts.xml"):
    mac_set = []
    tree = ET.ElementTree(file = config)
    root = tree.getroot()
    assert root.tag == "hosts"

    mac_set = []
    black_list = {}

    # parse black list
    for e in root.find("black").findall("a"):
       black_list[e.text.strip()] = True
    # parse hosts
    for e in root.find("macs").findall("a"):
        server = e.text.strip()
        if not black_list.has_key(server):
            mac_set.append(server)
    return mac_set

def start_servers(cmd,hosts,num,params):
    if len(hosts) == 0:
        return 0

    OUTPUT_CMD_LOG = " 1>log 2>&1 &" ## this will flush the log to a file
    """
    cmd:   The cmd which shall run on each server.
    hosts: All hosts in the configuration.
    num:   The number of servers to run.
    """
    for i in xrange(1,num):
        n_cmd = (cmd % (i)) + OUTPUT_CMD_LOG ## disable remote output

        if r.check_liveness(params,hosts[i]):
            r.kill_instances(params,hosts)
            #print("Failed to run due to machine failure.")
            #return False

        subprocess.call(["ssh", "-n","-f", hosts[i], n_cmd])
    zero_cmd = (cmd % (0)) + OUTPUT_CMD_LOG
    subprocess.call(["ssh", "-n","-f", hosts[0], zero_cmd])
    return

def copy_files(params,hosts,num):
    finished = []
    for i in xrange(0,num):
        subprocess.call(["scp",params["config"],"%s:%s" % (hosts[i],"~")],stdout=FNULL, stderr=subprocess.STDOUT)
        subprocess.call(["scp",params["exe"],"%s:%s" % (hosts[i],"~")],stdout=FNULL,stderr=subprocess.STDOUT)
        subprocess.call(["scp","hosts.xml","%s:%s" % (hosts[i],"~")],stdout=FNULL, stderr=subprocess.STDOUT)
        finished.append(hosts[i])
    return finished


def main():
    global r
    r = RoccRunner()

    signal.signal(signal.SIGINT, sigint_handler) ## register signal interrupt handler
    r.parse_trace()

    ## parse mac_set
    hosts = parse_hosts()

    ##
    for params in r:
        print(params)

        trace_macs = copy_files(params,hosts,params["m"])
        print(trace_macs)

        cmd = r.get_next_cmd(params)

        for i in xrange(3):
            ## run one trace
            start_servers(cmd,hosts,params["m"],params)
            time.sleep(5)

            ## wait until the trace end
            count = 0
            while r.check_liveness(params,hosts[0]):
                time.sleep(10)
                count += 1
                if count > 80:
                    ## failed case
                    print("error happens!")
                    break
            print("kill instances")
            r.kill_instances(params,hosts)
            r.append_res(params,i)

            time.sleep(25)
            #if not (r.append_res(params,i)):
             #   time.sleep(5) #
                #exit(0)

    pass
    print(r.results)
    r.dump_results("trace")

if __name__ == "__main__":
    main()
