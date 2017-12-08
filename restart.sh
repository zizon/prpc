#!/bin/bash
pid=`ps axu |grep sche | grep python | awk '{print $2}'`
kill -9 $pid
nohup ./schedule.py >/tmp/schedule.log 2>&1 &
