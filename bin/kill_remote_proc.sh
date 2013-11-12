#!/usr/bin/env bash
echo $1 $2

# ssh root@$1 -oStrictHostKeyChecking=no "\"kill -TERM $2\""

command=`ssh root@1 -oStrictHostKeyChecking=no "kill -9 $2"`
echo command
