#!/bin/bash
blocksync="`pwd`/../blocksync.py"
remotehost="example@localhost"
testhome="/tmp/blocksync/test"
filesizes="0 1 2 3 4 1024 4096 1048575 1048576 1048577 67108863 67108864 67108865"
skips="0 1 2 3 4 16 32 64 65 1024 1025"
hashalg="sha256"
hashcmd="$hashalg""sum"
error=0

mkdir -p "$testhome/src"
ssh "$remotehost" mkdir -p "$testhome/dst"
for filesize in $filesizes; do
    echo "Testing filesize $filesize"
    truncate --size=$filesize "$testhome/src/test.$filesize"
    srcfile="$testhome/src/test.$filesize"
    dstfile="$testhome/dst/test.$filesize"
    cmd="$blocksync $srcfile $remotehost $dstfile -f -a $hashalg"; $cmd >/dev/null
    sum1=`"$hashcmd" "$srcfile" | grep -o "^[[:alnum:]]*"`
    sum2=`ssh "$remotehost" "$hashcmd" "$dstfile" | grep -o "^[[:alnum:]]*"`
    if [ "$sum1" == "$sum2" ]; then
        res="PASS"
    else
        res="FAIL"
        error=1
    fi
    echo "CMD:  $cmd"
    echo "SUM1: $sum1 $srcfile"
    echo "SUM2: $sum2 $dstfile"
    echo "RES:  $res"
    echo
done

# block-skip test
for skip in $skips; do
    echo "Testing skip $skip"	
    cmd="$blocksync $srcfile $remotehost $dstfile -f -a $hashalg -k $skip"; $cmd >/dev/null
    sum2=`ssh "$remotehost" "$hashcmd" "$dstfile" | grep -o "^[[:alnum:]]*"`
    if [ "$sum1" == "$sum2" ]; then
        res="PASS"
    else
        res="FAIL"
        error=1
    fi
    echo "CMD:  $cmd"  
    echo "SUM1: $sum1 $srcfile"
    echo "SUM2: $sum2 $dstfile"
    echo "RES:  $res"
    echo
done

if [ $error -gt 0 ]; then
    echo "FINAL RESULT: FAIL"
    exit 1
else
    echo "FINAL RESULT: PASS"
    exit 0
fi
