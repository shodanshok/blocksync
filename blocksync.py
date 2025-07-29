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

# Comparison constants
SAME = "same"
DIFF = "diff"


# Checking for availables libs. If not found, disable the corresponding option
def check_available_libs():
    hostname = os.uname()[1]
    if options.nocache and not FADVISE_AVAILABLE:
        sys.stderr.write("Missing FADVISE library on "+hostname+"\n")
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
    # Open stdin in binary mode to avoid encoding issues
    if f == "-":
        f = open(0, 'rb')
        return f, 0
    # If dryrun, force open in read-only mode
    if options.dryrun:
        mode = 'rb'
    f = open(f, mode)
    f.seek(0, 2)
    size = f.tell()
    f.seek(options.skip*options.blocksize)
    return f, size


def create_file(f):
    if os.path.exists(dstpath):
        f = open(f, 'r+b')
    else:
        f = open(f, 'w+b')
    if options.devsize and not (os.path.getsize(dstpath) == options.devsize):
        f.truncate(options.devsize)
    f.close()


# Read, hash and put blocks on internal multiprocessing pipe
def getblocks(f):
    zeroblock = '\0'*options.blocksize
    while True:
        block = f.read(options.blocksize)
        if not block:
            break
        if block == zeroblock:
            csum = "0"
        else:
            csum = hashfunc(block).hexdigest()
        # fadvises
        if options.nocache:
            fadvise.posix_fadvise(f.fileno(),
                                  f.tell()-options.blocksize, options.blocksize,
                                  fadvise.POSIX_FADV_DONTNEED)
        if FADVISE_AVAILABLE:
            fadvise.posix_fadvise(f.fileno(), f.tell(), options.blocksize*4,
                                  fadvise.POSIX_FADV_WILLNEED)
        # return data
        yield (block, csum)


# This is the server (remote, or write-enabled) component
def server(dstpath):
    check_available_libs()
    # Should dst be created?
    if options.force:
        create_file(dstpath)
    # Open and read dst
    try:
        f, size = do_open(dstpath, 'r+b')
    except Exception as e:
        sys.stderr.write("ERROR: can not access destination path! %s\n" % e)
        sys.exit(1)
    # Begin comparison
    sys.stdout.write(dstpath+":"+str(options.blocksize)+":"+str(size)+"\n")
    sys.stdout.flush()
    block_id = options.skip
    for (block, csum) in getblocks(f):
        sys.stdout.write(csum+"\n")
        sys.stdout.flush()
        in_line = sys.stdin.buffer.readline()
        if not in_line:
            return
        in_line = in_line.decode().rstrip()
        res, complen = in_line.split(":")
        if res != SAME:
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


# Local component. It print current options and send SAME/DIFF flags to server
def sync(srcpath, dsthost, dstpath):
    # If dstpath is not specified, use the same name as srcpath
    if not dstpath:
        dstpath = srcpath
    # Open srcpath readonly
    try:
        f, size = do_open(srcpath, 'rb')
    except Exception as e:
        sys.stderr.write("ERROR: can not access source path! %s\n" % e)
        sys.exit(1)
    # Print a session summary
    print ("\n");
    print ("Dry run     : "+str(options.dryrun))
    print ("Local       : "+str(local))
    print ("Block size  : %0.1f KB" % (float(options.blocksize) / (1024)))
    print ("Skipped     : "+str(options.skip)+" blocks")
    print ("Hash alg    : "+options.hashalg)
    print ("Crypto alg  : "+options.encalg)
    print ("Compression : "+str(options.compress))
    print ("Read cache  : "+str(not options.nocache))
    print ("SRC command : "+" ".join(sys.argv))
    # Generate server command
    cmd = [__file__, 'server', dstpath, '-a', options.hashalg,
           '-b', str(options.blocksize), '-k', str(options.skip)]
    if options.sudo:
        cmd = ['sudo'] + cmd
    if not local:
        cmd = ['ssh', '-c', options.encalg, dsthost] + cmd
    # Extra options
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
    # Run remote command
    print ("DST command : "+" ".join(cmd))
    print ("\n")
    p = subprocess.Popen(cmd, bufsize=0,
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         close_fds=True)
    p_in, p_out = p.stdin, p.stdout
    # Sanity checks
    p.poll()
    line = p_out.readline().decode().rstrip()
    (remote_dstpath, remote_blocksize, remote_size) = line.split(":")
    remote_blocksize = int(remote_blocksize)
    remote_size = int(remote_size)
    if p.returncode is not None:
        sys.stderr.write("ERROR: connecting to or invoking blocksync on the remote host!\n\n")
        sys.exit(1)
    if remote_dstpath != dstpath:
        sys.stderr.write("ERROR: DST path (%s) doesn't match with the remote host (%s)!\n\n" %\
              (dstpath, remote_dstpath))
        sys.exit(1)
    if remote_blocksize != options.blocksize:
        sys.stderr.write("ERROR: SRC block size (%d) doesn't match with the remote (%d)!\n\n" %\
              (options.blocksize, remote_blocksize))
        sys.exit(1)
    if p.returncode is not None:
        sys.stderr.write("ERROR: can not access path on remote host!\n\n")
        sys.exit(1)
    if size > 0 and size != remote_size:
        sys.stderr.write("ERROR: SRC path size (%d) doesn't match DST path size (%d)!\n\n" %\
              (size, remote_size))
        sys.exit(1)
    # Start sync
    same_blocks = diff_blocks = 0
    print ("Synching...")
    t0 = time.time()
    t_last = t0
    size_blocks = size / options.blocksize
    if size_blocks < 1:
        size_blocks = 1
    if size_blocks * options.blocksize < size:
        size_blocks = size_blocks+1
    c_sum = hashfunc()
    block_id = options.skip
    for (l_block, l_sum) in getblocks(f):
        if options.showsum and not options.skip:
            c_sum.update(l_block)
        r_sum = p_out.readline().decode().rstrip()
        if l_sum == r_sum:
            p_in.write((SAME+":"+str(len(l_block))+"\n").encode())
            p_in.flush()
            same_blocks += 1
        else:
            if options.compress:
                l_block = compfunc(l_block)
            p_in.write((DIFF+":"+str(len(l_block))+"\n").encode())
            p_in.flush()
            p_in.write(l_block)
            p_in.flush()
            diff_blocks += 1
        t1 = time.time()
        if t1 - t_last > 1 or (options.skip + same_blocks + diff_blocks) >= size_blocks:
            rate = ((block_id - options.skip + 1.0) * options.blocksize /
                    (1024.0 * 1024.0) / (t1 - t0))
            show_stats(same_blocks, diff_blocks, size_blocks, rate)
            t_last = t1
        block_id = block_id+1
    # Sync pipes
    p.communicate()
    # Print final info
    print ("\n\nCompleted in %d seconds" % (time.time() - t0))
    if options.showsum:
        if options.skip:
            print ("Source checksum: N/A (skipped block detected)")
        else:
            print ("Source checksum: "+c_sum.hexdigest())
    return same_blocks, diff_blocks

# Show stats
def show_stats(same_blocks, diff_blocks, size_blocks, rate):
    sumstring = "\rskipped: %d, same: %d, diff: %d, %d/%d, %5.1f MB/s"
    if not options.quiet or (same_blocks + diff_blocks) >= size_blocks:
        print (sumstring % (options.skip, same_blocks, diff_blocks, 
                           options.skip + same_blocks + diff_blocks,
                           size_blocks, rate),end="")

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
    else:
        compfunc = None
        decompfunc = None
    return (compfunc, decompfunc)

# Main entry point
if __name__ == "__main__":
    parser = OptionParser(
        usage="%prog [options] /dev/source user@remotehost [/dev/dest]\n\
       %prog [options] /dev/source localhost /dev/dest")
    parser.add_option("-b", "--blocksize", dest="blocksize", action="store",
                      type="int", help="block size (bytes). Default: 128 KiB",
                      default=128 * 1024)
    parser.add_option("-a", "--hashalg", dest="hashalg", action="store",
                      type="string", help="Hash alg (md5, sha1, sha256, sha512)\
                      Default: sha256", default="sha256")
    parser.add_option("-e", "--encalg", dest="encalg", action="store",
                      type="string", help="SSH encryption alg. Default: aes128",
                      default="aes128-cbc")
    parser.add_option("-x", "--nocache", dest="nocache", action="store_true",
                      help="Minimize read cache usage. Default: off. \
                      NOTE: it requires the fadvise extension", default=False)
    parser.add_option("-c", "--showsum", dest="showsum", action="store_true",
                      help="Calculate and show complete source hashsum. \
                      Default: off", default=False)
    parser.add_option("-C", "--compress", dest="compress", action="store",
                      help="Use lzo lz4 or zstd compression for block transfer. \
                      Default: off", default=False)
    parser.add_option("-s", "--sudo", dest="sudo", action="store_true",
                      help="Use sudo. Defaul: off", default=False)
    parser.add_option("-f", "--force", dest="force", action="store_true",
                      help="Force transfer even if dst does not exist. \
                      Default: False", default=False)
    parser.add_option("-d", "--dryrun", dest="dryrun", action="store_true",
                      help="Dry run (do not alter destination file). \
                      Default: False", default=False)
    parser.add_option("-q", "--quiet", dest="quiet", action="store_true",
                      help="Quiet. Do not display progress. \
                      Default: False", default=False)
    parser.add_option("-k", "--skip", dest="skip", action="store",
                      type="int", help="Skip N blocks from the beginning. \
                      Default: 0", default=0)
    parser.add_option("--devsize", dest="devsize", action="store", type="int",
                      help="*INTERNAL USE ONLY* Specify dev/file size. \
                      Do NOT use it directly", default=False)
    (options, args) = parser.parse_args()

    check_available_libs()

    # Basic sanity check
    if len(args) < 2:
        parser.print_help()
        print (__doc__)
        sys.exit(1)

    # Check if right side is local or remote
    local = False
    if args[1] == "localhost":
        local = True
    if local and len(args) < 3:
        parser.print_help()
        print (__doc__)
        sys.exit(1)

    # Select hash function
    hashfunc = get_hashfunc()
    (compfunc, decompfunc) = get_compfunc()

    # Detect if server side is needed
    if args[0] == 'server':
        dstpath = args[1]
        server(dstpath)
    else:
        srcpath = args[0]
        dsthost = args[1]
        if len(args) > 2:
            dstpath = args[2]
        else:
            dstpath = srcpath
        sync(srcpath, dsthost, dstpath)
