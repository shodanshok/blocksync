#!/usr/bin/env python3
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
    sudo python3 blocksync.py /dev/source user@remotehost /dev/dest

- For local copy
* Simply run ./blocksync with 'localhost' as the target host
"""

#pylint: disable=E1101,E0401,E0606
#pylint: disable=W0702,W0621,W0703,W1514
#pylint: disable=C0111,C0103,C0209,R0914,R0912,R0915,R1732,I1101

# Imports
import os
import sys
import hashlib
import subprocess
import time
import argparse
import math
import zlib

try:
    callable(os.posix_fadvise)
    FADVISE_AVAILABLE = True
except:
    FADVISE_AVAILABLE = False

try:
    import lzo
    LZO_AVAILABLE = True
except:
    LZO_AVAILABLE = False

try:
    import lz4
    LZ4_AVAILABLE = True
    # Newer lz4 bindings (as the FreeBSD one) require importing a sub-packages
    try:
        import lz4.block
        lz4.compress = lz4.block.compress
        lz4.decompress = lz4.block.decompress
    except:
        pass
except:
    LZ4_AVAILABLE = False

try:
    import zstd
    ZSTD_AVAILABLE = True
except:
    ZSTD_AVAILABLE = False

# Constants
SAME = "same"
DIFF = "diff"


# Checking for availables libs. If not found, disable the corresponding option
def check_available_libs():
    hostname = os.uname()[1]
    if options.nocache and not FADVISE_AVAILABLE:
        sys.stderr.write("Missing FADVISE support on "+hostname+"\n")
        sys.exit(1)
    if options.compress == "lzo" and not LZO_AVAILABLE:
        sys.stderr.write("Missing LZO library on "+hostname+"\n")
        sys.exit(1)
    if options.compress == "lz4" and not LZ4_AVAILABLE:
        sys.stderr.write("Missing LZ4 library on "+hostname+"\n")
        sys.exit(1)
    if options.compress == "zstd" and not ZSTD_AVAILABLE:
        sys.stderr.write("Missing ZSTD library on "+hostname+"\n")
        sys.exit(1)


# Open file/dev
def do_open(f, mode):
    # If dryrun, force open in read-only mode
    if options.dryrun:
        mode = 'rb'
    f = open(f, mode)
    f.seek(0, 2)
    size = f.tell()
    if FADVISE_AVAILABLE:
        os.posix_fadvise(f.fileno(), f.tell(), 0, os.POSIX_FADV_SEQUENTIAL)
    f.seek(options.skip*options.blocksize)
    return f, size


def create_file(f):
    if os.path.exists(dstpath):
        f = open(f, 'r+b')
    else:
        f = open(f, 'w+b')
    if options.devsize and not os.path.getsize(dstpath) == options.devsize:
        f.truncate(options.devsize)
    f.close()


# Read, hash and put blocks on internal multiprocessing pipe
def getblocks(f):
    while True:
        block = f.read(options.blocksize)
        if not block:
            break
        csum = hashfunc(block).hexdigest()
        if options.nocache:
            cache = max(options.blocksize*4, 1024*1024)
            os.posix_fadvise(f.fileno(), f.tell()-cache, cache,
                             os.POSIX_FADV_DONTNEED)
        yield (block, csum)


def print_session():
    print ("\n")
    print ("Dry run     : "+str(options.dryrun))
    print ("Local       : "+str(local))
    print ("Block size  : %0.1f KB" % (float(options.blocksize) / (1024)))
    print ("Skipped     : "+str(options.skip)+" blocks")
    print ("Hash alg    : "+options.hashalg)
    print ("Crypto alg  : "+options.encalg)
    print ("Compression : "+str(options.compress))
    print ("Read cache  : "+str(not options.nocache))
    print ("SRC command : "+" ".join(sys.argv))


def print_stats(same_blocks, diff_blocks, size_blocks, rate):
    sumstring = "\rskipped: %d, same: %d, diff: %d, %d/%d, %5.1f MB/s"
    if options.quiet:
        return
    print (sumstring % (options.skip, same_blocks, diff_blocks,
                        options.skip + same_blocks + diff_blocks,
                        size_blocks, rate),end="")

def print_epilog(t_start):
    print ("\n\nCompleted in %d seconds" % (time.time() - t_start))
    if options.showsum:
        if options.skip:
            print ("Source checksum: N/A (skipped block detected)")
        else:
            print ("Source checksum: "+hashfunc().hexdigest())


def generate_command(size):
    if srchost:
        host = srchost
        cmd = [__file__, srcpath, "stdout", "--reader", '-a', options.hashalg,
               '-b', str(options.blocksize), '-k', str(options.skip)]
    else:
        host = dsthost
        cmd = [__file__, "stdin", dstpath, "--writer", '-a', options.hashalg,
               '-b', str(options.blocksize), '-k', str(options.skip)]
    if options.sudo:
        cmd = ['sudo'] + cmd
    if not local:
        cmd = ['ssh', '-c', options.encalg, host] + cmd
    if options.nocache:
        cmd.append("-x")
    if options.compress:
        cmd.append("--compress="+options.compress)
    if options.force:
        cmd.append("-f")
        cmd.append("--devsize")
        cmd.append(str(size))
    if options.dryrun:
        cmd.append("-d")
    print ("DST command : "+" ".join(cmd))
    print ("\n")
    return cmd


def sanity_check(size, p):
    p.poll()
    line = p.stdout.readline().decode().rstrip()
    (child_path, child_blocksize, child_size) = line.split(":")
    child_blocksize = int(child_blocksize)
    child_size = int(child_size)
    if srchost:
        path = srcpath
    else:
        path = dstpath
    if p.returncode is not None:
        sys.stderr.write("ERROR: connecting to or invoking child blocksync process!\n\n")
        return False
    if child_path != path:
        sys.stderr.write("ERROR: local path (%s) doesn't match with child (%s)!\n\n" %\
                (path, child_path))
        return False
    if child_blocksize != options.blocksize:
        sys.stderr.write("ERROR: local block size (%d) doesn't match with child (%d)!\n\n" %\
                (options.blocksize, child_blocksize))
        return False
    if p.returncode is not None:
        sys.stderr.write("ERROR: can not access path from child process!\n\n")
        return False
    if size > 0 and size != child_size:
        sys.stderr.write("ERROR: local path size (%d) doesn't match child path size (%d)!\n\n" %\
                (size, child_size))
        return False
    return True


def child():
    check_available_libs()
    if options.reader:
        path = srcpath
        try:
            f, size = do_open(srcpath, 'rb')
        except Exception as e:
            sys.stderr.write("ERROR: can not access source path! %s\n" % e)
            sys.exit(1)
    else:
        path = dstpath
        if options.force:
            create_file(dstpath)
        try:
            f, size = do_open(dstpath, 'r+b')
        except Exception as e:
            sys.stderr.write("ERROR: can not access destination path! %s\n" % e)
            sys.exit(1)
    # Begin comparison
    sys.stdout.write(path+":"+str(options.blocksize)+":"+str(size)+"\n")
    sys.stdout.flush()
    block_id = options.skip
    for (block, csum) in getblocks(f):
        sys.stdout.write(csum+"\n")
        sys.stdout.flush()
        (res, complen) = sys.stdin.buffer.readline().decode().rstrip().split(":")
        if res != SAME:
            if options.reader:
                if options.compress:
                    block = compfunc(block)
                sys.stdout.write(csum+":"+str(len(block))+"\n")
                sys.stdout.flush()
                sys.stdout.buffer.write(block)
                sys.stdout.flush()
            else:
                if options.compress:
                    block = decompfunc(sys.stdin.buffer.read(int(complen)))
                else:
                    block = sys.stdin.buffer.read(options.blocksize)
                # Do not write anything if dryrun
                if not options.dryrun:
                    f.seek(block_id*options.blocksize, 0)
                    f.write(block)
                    f.flush()
        block_id = block_id+1


# Local component. It print current options and send SAME/DIFF flags to child
def sync():
    if srchost:
        path = dstpath
        mode = "r+b"
    else:
        path = srcpath
        mode = "rb"
    try:
        f, size = do_open(path, mode)
    except Exception as e:
        sys.stderr.write("ERROR: can not access path! %s\n" % e)
        sys.exit(1)
    # Print session summary
    print_session()
    # Generate server command
    cmd = generate_command(size)
    # Run remote command
    p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    # Sanity checks
    if not sanity_check(size, p):
        sys.exit(1)
    # Start sync
    print ("Synching...")
    same_blocks = diff_blocks = 0
    t_start = t_last = time.time()
    size_blocks = max(math.ceil(size/options.blocksize), 1)
    block_id = options.skip
    for (l_block, l_sum) in getblocks(f):
        #if options.showsum and not options.skip:
        #    hashfunc().update(l_block)
        r_sum = p.stdout.readline().decode().rstrip()
        if l_sum == r_sum:
            p.stdin.write((SAME+":"+str(len(l_block))+"\n").encode())
            p.stdin.flush()
            same_blocks += 1
        else:
            if srchost:
                p.stdin.write((DIFF+":"+str(len(l_block))+"\n").encode())
                p.stdin.flush()
                (r_sum, r_len) = p.stdout.readline().decode().rstrip().split(":")
                r_block = p.stdout.read(int(r_len))
                if options.compress:
                    r_block = decompfunc(r_block)
                if not options.dryrun:
                    f.seek(block_id*options.blocksize, 0)
                    f.write(r_block)
                    f.flush()
            else:
                if options.compress:
                    l_block = compfunc(l_block)
                p.stdin.write((DIFF+":"+str(len(l_block))+"\n").encode())
                p.stdin.flush()
                p.stdin.write(l_block)
                p.stdin.flush()
            diff_blocks += 1
        t_now = time.time()
        block_id = block_id+1
        if t_now - t_last > 1 or block_id >= size_blocks:
            rate = ((block_id - options.skip + 1.0) * options.blocksize /
                    (1024.0 * 1024.0) / (t_now - t_start))
            print_stats(same_blocks, diff_blocks, size_blocks, rate)
            t_last = t_now
    # Sync pipes
    p.communicate()
    # Print final info
    print_epilog(t_start)


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

# Dynamically loaded compression function
def get_compfunc():
    if options.compress == "lz4":
        compfunc = lz4.compress
        decompfunc = lz4.decompress
    elif options.compress == "lzo":
        compfunc = lzo.compress
        decompfunc = lzo.decompress
    elif options.compress == "zstd":
        compfunc = zstd.compress
        decompfunc = zstd.decompress
    elif options.compress == "zlib":
        compfunc = zlib.compress
        decompfunc = zlib.decompress
    else:
        compfunc = None
        decompfunc = None
    return (compfunc, decompfunc)

# arguments
parser = argparse.ArgumentParser()
parser.add_argument("src", help="Source file, ie: /home/example/file.img")
parser.add_argument("rem", nargs="?", help="Legacy remote host format")
parser.add_argument("dst", help="Destination host/file, ie: example@remote:/home/example/file.img")
parser.add_argument("-b", "--blocksize", action="store", default=128*1024,
                    type=int, help="block size (bytes). Default: 128 KiB")
parser.add_argument("-a", "--hashalg", action="store", default="sha256",
                    help="Hash alg: md5 sha1 sha256 sha512. Default: sha256")
parser.add_argument("-e", "--encalg", action="store", default="aes128-ctr,aes128-cbc",
                    help="SSH encryption alg. Default: aes128")
parser.add_argument("-x", "--nocache", action="store_true", default=False,
                    help="Minimize read cache usage. Default: off")
parser.add_argument("-c", "--showsum", action="store_true", default=False,
                    help="Show complete source hashsum. Default: off")
parser.add_argument("-C", "--compress", action="store", default=False,
                    help="Compression: lzo lz4 zstd zlib. Default: off")
parser.add_argument("-s", "--sudo", action="store_true", default=False,
                    help="Use sudo. Defaul: off")
parser.add_argument("-f", "--force", action="store_true", default=False,
                    help="Force transfer even if dst does not exist. Default: off")
parser.add_argument("-d", "--dryrun", action="store_true", default=False,
                    help="Dry run. Default: off")
parser.add_argument("-q", "--quiet", action="store_true", default=False,
                    help="Do not display progress. Default: off")
parser.add_argument("-k", "--skip", action="store", default=0, type=int,
                    help="Skip N blocks from the beginning. Default: 0")
parser.add_argument("--writer", action="store_true", default=False,
                    help="*INTERNAL USE ONLY* Specify server mode. Do NOT use it directly")
parser.add_argument("--reader", action="store_true", default=False,
                    help="*INTERNAL USE ONLY* Specify server mode. Do NOT use it directly")
parser.add_argument("--devsize", action="store", default=False, type=int,
                    help="*INTERNAL USE ONLY* Specify dev/file size. Do NOT use it directly")
options = parser.parse_args()
check_available_libs()

# Select hash function
hashfunc = get_hashfunc()
(compfunc, decompfunc) = get_compfunc()

# Global vars
local = False
srchost = False
srcpath = False
dsthost = False
dstpath = False

# Params parsing
if "@" in options.src and ":" in options.src:
    (srchost, srcpath) = options.src.split(':')
else:
    srcpath = options.src
if "@" in options.dst and ":" in options.dst:
    (dsthost, dstpath) = options.dst.split(':')
else:
    dstpath = options.dst
if options.rem:
    dsthost = options.rem
if not dstpath:
    dstpath = srcpath
if not srchost and not dsthost or dsthost == "localhost":
    local = True
    if srcpath == dstpath:
        parser.print_help()
        sys.exit(1)

# Start sync
if options.reader or options.writer:
    child()
else:
    sync()
