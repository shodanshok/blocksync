#!/usr/bin/env python
"""
Synchronise block devices over the network

Copyright 2006-2008 Justin Azoff <justin@bouncybouncy.net>
Copyright 2011 Robert Coup <robert@coup.net.nz>
License: GPL

Getting started:

* Copy blocksync.py to the home directory on the remote host
* Make sure your remote user can either sudo or is root itself.
* Make sure your local user can ssh to the remote host
* Invoke:
    sudo python blocksync.py /dev/source user@remotehost /dev/dest
"""

#pylint: disable=E1101
#pylint: disable=W0702,W0621,W0703
#pylint: disable=C0111,C0103,R0914,R0912,R0915

import sys
import hashlib
import subprocess
import time
from optparse import OptionParser

try:
    import fadvise
    FADVISEABLE = True
except:
    FADVISEABLE = False

try:
    import lzo
except:
    sys.stderr.write("Missing LZO library. \
                     Please run pip 'install python-lzo' \
                     on both server and client\n")
    quit(1)

SAME = "same"
DIFF = "diff"


def do_open(f, mode):
    f = open(f, mode)
    f.seek(0, 2)
    size = f.tell()
    f.seek(0)
    return f, size


def getblocks(f):
    while 1:
        block = f.read(options.blocksize)
        if not block:
            break
        if options.nocache and FADVISEABLE:
            fadvise.posix_fadvise(f.fileno(),
                                  f.tell()-options.blocksize, options.blocksize,
                                  fadvise.POSIX_FADV_DONTNEED)
        yield block


def server(dev):
    print dev, options.blocksize
    f, size = do_open(dev, 'r+')
    print size
    sys.stdout.flush()

    for block in getblocks(f):
        print hashfunc(block).hexdigest()
        sys.stdout.flush()
        res, complen = sys.stdin.readline().split(":")
        if res != SAME:
            if options.compress:
                newblock = lzo.decompress(sys.stdin.read(int(complen)))
            else:
                newblock = sys.stdin.read(options.blocksize)
            f.seek(-len(newblock), 1)
            f.write(newblock)
            f.flush()


def sync(srcdev, dsthost, dstdev):

    if not dstdev:
        dstdev = srcdev

    print "Block size  : %0.1f KB" % (float(options.blocksize) / (1024))
    print "Hash alg    : "+options.hashalg
    print "Crypto alg  : "+options.encalg
    print "Compression : "+str(options.compress)

    cmd = ['ssh', '-c', options.encalg, dsthost, 'python', 'blocksync.py',
           'server', dstdev, '-a', options.hashalg, '-b',
           str(options.blocksize)]
    if options.sudo:
        cmd = ['ssh', '-c', options.encalg, dsthost, 'sudo',
               'python', 'blocksync.py', 'server', dstdev,
               '-a', options.hashalg, '-b', str(options.blocksize)]
    if options.nocache:
        cmd.append("-x")
    if options.compress:
        cmd.append("-C")

    print "Running     : %s" % " ".join(cmd)

    p = subprocess.Popen(cmd, bufsize=0,
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         close_fds=True)
    p_in, p_out = p.stdin, p.stdout

    line = p_out.readline()
    p.poll()
    if p.returncode is not None:
        print "Error connecting to or invoking blocksync on the remote host!"
        sys.exit(1)

    a, b = line.split()
    if a != dstdev:
        print "DST device (%s) doesn't match with the remote host (%s)!" %\
              (dstdev, a)
        sys.exit(1)
    if int(b) != options.blocksize:
        print "SRC block size (%d) doesn't match with the remote host (%d)!" %\
              (options.blocksize, int(b))
        sys.exit(1)

    try:
        f, size = do_open(srcdev, 'r')
    except Exception, e:
        print "Error accessing source device! %s" % e
        sys.exit(1)

    line = p_out.readline()
    p.poll()
    if p.returncode is not None:
        print "Error accessing device on remote host!"
        sys.exit(1)
    remote_size = int(line)
    if size != remote_size:
        print "SRC device size (%d) doesn't match DST device size (%d)!" %\
              (size, remote_size)
        sys.exit(1)

    same_blocks = diff_blocks = 0

    print "Starting sync..."
    t0 = time.time()
    t_last = t0
    size_blocks = size / options.blocksize
    if size_blocks * options.blocksize < size:
        size_blocks = size_blocks+1
    c_sum = hashfunc()
    for i, l_block in enumerate(getblocks(f)):
        if options.showsum:
            c_sum.update(l_block)

        l_sum = hashfunc(l_block).hexdigest()
        r_sum = p_out.readline().strip()

        if l_sum == r_sum:
            p_in.write(SAME+":"+str(len(l_block))+"\n")
            p_in.flush()
            same_blocks += 1
        else:
            if options.compress:
                l_block = lzo.compress(l_block)
            p_in.write(DIFF+":"+str(len(l_block))+"\n")
            p_in.flush()
            p_in.write(l_block)
            p_in.flush()
            diff_blocks += 1

        t1 = time.time()
        if t1 - t_last > 1 or (same_blocks + diff_blocks) >= size_blocks:
            rate = (i + 1.0) * options.blocksize / (1024.0 * 1024.0) / (t1 - t0)
            print "\rsame: %d, diff: %d, %d/%d, %5.1f MB/s" %\
                  (same_blocks, diff_blocks, same_blocks + diff_blocks,
                   size_blocks, rate),
            t_last = t1

    print "\n\nCompleted in %d seconds" % (time.time() - t0)
    if options.showsum:
        print "Source checksum: "+c_sum.hexdigest()

    return same_blocks, diff_blocks

def get_hashfunc():
    hashalg = options.hashalg
    if hashalg == "md5":
        hashfunc = hashlib.md5
    elif hashalg == "sha1":
        hashfunc = hashlib.sha1
    elif hashalg == "sha256":
        hashfunc = hashlib.sha256
    else:
        hashfunc = hashlib.sha512

    return hashfunc

if __name__ == "__main__":
    parser = OptionParser(
        usage="%prog [options] /dev/source user@remotehost [/dev/dest]")
    parser.add_option("-b", "--blocksize", dest="blocksize", action="store",
                      type="int", help="block size (bytes). Default: 1 MiB",
                      default=1024 * 1024)
    parser.add_option("-a", "--hashalg", dest="hashalg", action="store",
                      type="string", help="Hash alg (md5, sha1, sha256, sha512). \
                      Default: sha512", default="sha512")
    parser.add_option("-e", "--encalg", dest="encalg", action="store",
                      type="string", help="SSH encryption alg. Default: aes128",
                      default="aes128-cbc")
    parser.add_option("-x", "--nocache", dest="nocache", action="store_true",
                      help="Minimize read cache usage. Default: off. \
                      NOTE: it requires the fadvise extension", default=False)
    parser.add_option("-c", "--showsum", dest="showsum", action="store_true",
                      help="Calculate and show complete source hashsum. \
                      Default: off", default=False)
    parser.add_option("-C", "--compress", dest="compress", action="store_true",
                      help="Use LZO compression for block transfer. \
                      Default: off", default=False)
    parser.add_option("-s", "--sudo", dest="sudo", action="store_true",
                      help="Use sudo. Defaul: off", default=False)
    (options, args) = parser.parse_args()

    if len(args) < 2:
        parser.print_help()
        print __doc__
        sys.exit(1)

    hashfunc = get_hashfunc()

    if args[0] == 'server':
        dstdev = args[1]
        server(dstdev)
    else:
        srcdev = args[0]
        dsthost = args[1]
        if len(args) > 2:
            dstdev = args[2]
        else:
            dstdev = srcdev
        sync(srcdev, dsthost, dstdev)
