#!/usr/bin/env bash

target=wxd@val01
## this script will sync the project to the remote server
rsync -i -rtuv $PWD/../ $target:~/projects/rocc_nvm  --exclude ./pre-data/
#rsync -e "ssh -i ../aws/tp.pem"  -rtuv $PWD/../ $target:~/nocc  --exclude ./pre-data/
