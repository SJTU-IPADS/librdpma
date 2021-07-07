# LibRDPMA, tools to analysis the performance when accessing NVM with RDMA

LibRDPMA provides a set of tools  to analyze the behavior when accessing NVM (i.e., Intel Optane DC persistent memory) with RDMA. These including benchmarks for one-sided RDMA and two-sided RDMA and tools to analyze NVM behavior. 



## Getting Started

Building the tools of librdpma is straightforward since it will automatically install dependencies. Specifically, using the following steps:

- Clone the project with `git clone https://github.com/SJTU-IPADS/librdpma.git --recursive`
- Make sure `cmake` is installed. Then execute `cmake .`
- Build the project with `make`. 



### Running benchmarks

For the built binaries, `./nvm_rrtserver` and ``./nvm_rrtclient` are used for evaluating two-sided performance, while `./nvm_server` and `./nvm_client` are used for evaluating one-sided performance. We provide scripts to run experiments. For how to configure these binaries, please use `binary --help` to check.

We've also provide scripts to run experiments. For example, to run one-sided evaluations, use the following:

- `cd scripts; ./bootstrap-proxy.py -f run_one.toml`; Note that `run_one.toml` should be configured according to your hardawre setting. It is straightforward to configure it  based on its content. 

  

### Other tools

We provide some tools for make system configurations or monitor NVM statistics. 

- To tune DDIO setups, use `cd ddio_tools; cmake; make;` and then use `setup_dca`. 

- To monitor NVM read/write amplications, use `cd nvm; python analysis.py`.  Note that `ipmctl` should be installed. 



## Check our results

To check the results of these benchmarks, please refer to our paper: 

[**ATC**] Characterizing and Optimizing Remote Persistent Memory with RDMA and NVM. Xingda Wei and Xiating Xie and Rong Chen and Haibo Chen and Binyu Zang. 2021 USENIX Annual Technical Conference. 

