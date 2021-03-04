#!/usr/bin/env bash

hse kvs destroy mp0/ksv0
sudo mpool destroy mp0

sudo mpool create mp0 /dev/nvme0n1 uid=tpartin
hse kvdb create mp0
hse kvs create mp0/ksv0
