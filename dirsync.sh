#!/bin/bash
# Example script to non-recursively backup an entire directory.
# If files are in-use, a snapshot (ie: via ZFS) should be taken.

# Variables
srcdir="/tank/kvm/.zfs/snapshot/backup/var/lib/libvirt/images"
dstdir="/mnt/vmbackup"
dsthost=127.0.0.1
spaced=false
local_exit_code=0
global_exit_code=0

# Snapshot and backup
zfs snapshot tank/kvm@backup
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

# Release snapshot and exit
zfs destroy tank/kvm@backup
exit $global_exit_code
