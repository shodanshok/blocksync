#!/bin/bash

srcdir="/tmp/test"
dstdir="/tmp/newtest"
dsthost=172.31.255.1
spaced=false
local_exit_code=0
global_exit_code=0

oldIFS=$IFS; IFS=$'\n'
for image in `ls $srcdir`; do
	echo $image | grep -q "[[:space:]]" && spaced=true
	$spaced && global_exit_code=2 && echo "Filename with spaces are not supported. Skipping $image" && continue
	/root/blocksync.py -f $srcdir/$image $dsthost $dstdir/$image; local_exit_code=$?
	if [ $local_exit_code -ne 0 ]; then
		global_exit_code=$local_exit_code
	fi
done
IFS=$oldIFS

exit $global_exit_code
