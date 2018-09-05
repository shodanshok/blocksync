#!/usr/bin/env python
"""
Synchronize dev/files over the network or locally

Copyright 2006-2008 Justin Azoff <justin@bouncybouncy.net>
Copyright 2011 Robert Coup <robert@coup.net.nz>
Copyright 2018 Gionatan Danti <g.danti@assyoma.it>
License: GPL

Getting started:

- For network copy
* Copy blocksync.py to the home directory on the remote host
* Make sure your remote user can either sudo or is root itself.
* Make sure your local user can ssh to the remote host
* Invoke:
    sudo python blocksync.py /dev/source user@remotehost /dev/dest

- For local copy
* Simply run ./blocksync with 'localhost' as the target device
"""

#pylint: disable=E1101
#pylint: disable=W0702,W0621,W0703
#pylint: disable=C0111,C0103,R0914,R0912,R0915

# Imports
import os
import sys
import hashlib
import subprocess
import time
from optparse import OptionParser
import multiprocessing

try:
    import fadvise
    FADVISE_AVAILABLE = True
except:
    FADVISE_AVAILABLE = False

try:
    import lzo
    LZO_AVAILABLE = True
except:
    LZO_AVAILABLE = False

# Comparison constants
SAME = "same"
DIFF = "diff"


# Checking for availables libs. If not found, disable the corresponding option
def check_available_libs():
    if options.nocache and not FADVISE_AVAILABLE:
        options.nocache = False
        sys.stderr.write("\n\
            Missing FADVISE library.\n\
            Please run 'pip install fadvise' on both client and server.\n\
            Continuing without FADVISE...\n")
    if options.compress and not LZO_AVAILABLE:
        options.compress = False
        sys.stderr.write("\n\
            Missing LZO library.\n\
            Please run 'pip install python-lzo' on both client and server.\n\
            Continuing without LZO...\n")


# Open file/dev
def do_open(f, mode):
    f = open(f, mode)
    f.seek(0, 2)
    size = f.tell()
    f.seek(0)
    return f, size


def create_file(dev):
    f = open(dev, 'w+')
    if options.devsize > 0:
        f.seek(options.devsize-1)
        f.write("\0")
    f.close()


# Read, hash and put blocks on internal multiprocessing pipe
def getblocks(dev, pipe):
    f, dummy = do_open(dev, 'r')
    while True:
        block = f.read(options.blocksize)
        if not block:
            break
        csum = hashfunc(block).hexdigest()
        pipe.send((csum, block))
        # fadvises
        if options.nocache:
            fadvise.posix_fadvise(f.fileno(),
                                  f.tell()-options.blocksize, options.blocksize,
                                  fadvise.POSIX_FADV_DONTNEED)
        if FADVISE_AVAILABLE:
            fadvise.posix_fadvise(f.fileno(), f.tell(), options.blocksize*4,
                                  fadvise.POSIX_FADV_WILLNEED)


# This is the server (remote, or write-enabled) component
def server(dev):
    if not os.path.exists(dev) and options.force:
        create_file(dev)
    print dev, options.blocksize
    f, size = do_open(dev, 'r+')
    print size
    sys.stdout.flush()
    parent, child = multiprocessing.Pipe(False)
    reader = multiprocessing.Process(target=getblocks, args=(dev, child))
    reader.daemon = True
    reader.start()
    child.close()
    block_id = 0
    while True:
        try:
            (csum, block) = parent.recv()
        except:
            break
        print csum
        sys.stdout.flush()
        in_line = sys.stdin.readline()
        res, complen = in_line.split(":")
        if res != SAME:
            if options.compress:
                block = lzo.decompress(sys.stdin.read(int(complen)))
            else:
                block = sys.stdin.read(options.blocksize)
            f.seek(block_id*options.blocksize, 0)
            f.write(block)
            f.flush()
        block_id = block_id+1


# Local component. It print current options and send SAME/DIFF flags to server
def sync(srcdev, dsthost, dstdev):
    # If dstdev is not specified, use the same name as srcdev
    if not dstdev:
        dstdev = srcdev
    # Open srcdev readonly
    try:
        dummy, size = do_open(srcdev, 'r')
    except Exception, e:
        print "Error accessing source device! %s" % e
        sys.exit(1)
    # Print a session summary
    print
    print "Local       : "+str(local)
    print "Block size  : %0.1f KB" % (float(options.blocksize) / (1024))
    print "Hash alg    : "+options.hashalg
    print "Crypto alg  : "+options.encalg
    print "Compression : "+str(options.compress)
    print "Read cache  : "+str(not options.nocache)
    # Generate server command
    cmd = ['python', 'blocksync.py', 'server', dstdev, '-a', options.hashalg,
           '-b', str(options.blocksize)]
    if options.sudo:
        cmd = ['sudo'] + cmd
    if not local:
        cmd = ['ssh', '-c', options.encalg, dsthost] + cmd
    # Extra options
    if options.nocache:
        cmd.append("-x")
    if options.compress:
        cmd.append("-C")
    if options.force:
        cmd.append("-f")
        cmd.append("--devsize")
        cmd.append(str(size))
    # Run remote command
    print "Running     : %s" % " ".join(cmd)
    p = subprocess.Popen(cmd, bufsize=0,
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         close_fds=True)
    p_in, p_out = p.stdin, p.stdout
    # Sanity checks
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
    # Start sync
    same_blocks = diff_blocks = 0
    print
    print "Starting sync..."
    parent, child = multiprocessing.Pipe(False)
    reader = multiprocessing.Process(target=getblocks, args=(srcdev, child))
    reader.daemon = True
    reader.start()
    child.close()
    t0 = time.time()
    t_last = t0
    size_blocks = size / options.blocksize
    if size_blocks * options.blocksize < size:
        size_blocks = size_blocks+1
    c_sum = hashfunc()
    block_id = 0
    while True:
        try:
            (l_sum, l_block) = parent.recv()
        except:
            break
        if options.showsum:
            c_sum.update(l_block)
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
            rate = ((block_id + 1.0) * options.blocksize / (1024.0 * 1024.0) /
                    (t1 - t0))
            print "\rsame: %d, diff: %d, %d/%d, %5.1f MB/s" %\
                  (same_blocks, diff_blocks, same_blocks + diff_blocks,
                   size_blocks, rate),
            t_last = t1
        block_id = block_id+1
    # Print final info
    print "\n\nCompleted in %d seconds" % (time.time() - t0)
    if options.showsum:
        print "Source checksum: "+c_sum.hexdigest()
    return same_blocks, diff_blocks


# Dynamically loaded hash function
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


# Main entry point
if __name__ == "__main__":
    parser = OptionParser(
        usage="%prog [options] /dev/source user@remotehost [/dev/dest]\n\
       %prog [options] /dev/source localhost /dev/dest")
    parser.add_option("-b", "--blocksize", dest="blocksize", action="store",
                      type="int", help="block size (bytes). Default: 1 MiB",
                      default=1024 * 1024)
    parser.add_option("-a", "--hashalg", dest="hashalg", action="store",
                      type="string", help="Hash alg (md5, sha1, sha256, sha512)\
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
    parser.add_option("-f", "--force", dest="force", action="store_true",
                      help="Force transfer even if dst does not exist. \
                      Default: False", default=False)
    parser.add_option("--devsize", dest="devsize", action="store", type="int",
                      help="*INTERNAL USE ONLY* Specify dev/file size. \
                      Do NOT use it directly", default=False)
    (options, args) = parser.parse_args()

    check_available_libs()

    # Basic sanity check
    if len(args) < 2:
        parser.print_help()
        print __doc__
        sys.exit(1)

    # Check if right side is local or remote
    local = False
    if args[1] == "localhost":
        local = True
    if local and len(args) < 3:
        parser.print_help()
        print __doc__
        sys.exit(1)

    # Select hash function
    hashfunc = get_hashfunc()

    # Detect if server side is needed
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
