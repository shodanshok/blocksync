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
 
import sys
import hashlib
import subprocess
import time

try:
    import fadvise
    fadviseable = True
except:
    fadviseable = False
    pass

try:
    import lzo
except:
    sys.stderr.write("Missing LZO library. Please run pip 'install python-lzo' on both server and client\n")
    quit(1)

SAME = "same"
DIFF = "diff"
 
def do_open(f, mode):
    f = open(f, mode)
    f.seek(0, 2)
    size = f.tell()
    f.seek(0)
    return f, size
 
 
def getblocks(f, blocksize, nocache):
    while 1:
        block = f.read(blocksize)
        if not block:
            break
        if nocache and fadviseable:
            fadvise.posix_fadvise(f.fileno(), f.tell()-blocksize, blocksize, fadvise.POSIX_FADV_DONTNEED)
        yield block
 
 
def server(dev, blocksize, nocache, compress):
    print dev, blocksize
    f, size = do_open(dev, 'r+')
    print size
    sys.stdout.flush()
 
    for block in getblocks(f, blocksize, nocache):
        print hashfunc(block).hexdigest()
        sys.stdout.flush()
        res, complen = sys.stdin.readline().split(":")
        if res != SAME:
            if compress:
	            newblock = lzo.decompress(sys.stdin.read(int(complen)))
            else:
                    newblock = sys.stdin.read(blocksize)
            f.seek(-len(newblock), 1)
            f.write(newblock)
            f.flush()
 
 
def sync(srcdev, dsthost, dstdev, blocksize, hashalg, encalg, nocache, showsum, compress, sudo):
 
    if not dstdev:
        dstdev = srcdev
 
    print "Block size  : %0.1f MB" % (float(blocksize) / (1024 * 1024))
    print "Hash alg    : "+hashalg
    print "Crypto alg  : "+encalg
    print "Compression : "+str(compress)

    cmd = ['ssh', '-c', encalg, dsthost, 'python', 'blocksync.py', 'server', dstdev, '-a', hashalg, '-b', str(blocksize)]
    if sudo:
        cmd = ['ssh', '-c', encalg, dsthost, 'sudo', 'python', 'blocksync.py', 'server', dstdev, '-a', hashalg, '-b', str(blocksize)]
    if nocache:
        cmd.append("-x")
    if compress:
        cmd.append("-C")

    print "Running     : %s" % " ".join(cmd)
 
    p = subprocess.Popen(cmd, bufsize=0, stdin=subprocess.PIPE, stdout=subprocess.PIPE, close_fds=True)
    p_in, p_out = p.stdin, p.stdout
 
    line = p_out.readline()
    p.poll()
    if p.returncode is not None:
        print "Error connecting to or invoking blocksync on the remote host!"
        sys.exit(1)
 
    a, b = line.split()
    if a != dstdev:
        print "Dest device (%s) doesn't match with the remote host (%s)!" % (dstdev, a)
        sys.exit(1)
    if int(b) != blocksize:
        print "Source block size (%d) doesn't match with the remote host (%d)!" % (blocksize, int(b))
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
        print "Source device size (%d) doesn't match remote device size (%d)!" % (size, remote_size)
        sys.exit(1)
 
    same_blocks = diff_blocks = 0
 
    print "Starting sync..."
    t0 = time.time()
    t_last = t0
    size_blocks = size / blocksize
    if size_blocks * blocksize < size:
        size_blocks = size_blocks+1
    c_sum = hashfunc()
    for i, l_block in enumerate(getblocks(f, blocksize, nocache)):
        if showsum:
            c_sum.update(l_block)

        l_sum = hashfunc(l_block).hexdigest()
        r_sum = p_out.readline().strip()

        if l_sum == r_sum:
            p_in.write(SAME+":"+str(len(l_block))+"\n")
            p_in.flush()
            same_blocks += 1
        else:
            if compress:
                l_block = lzo.compress(l_block)
            p_in.write(DIFF+":"+str(len(l_block))+"\n")
            p_in.flush()
            p_in.write(l_block)
            p_in.flush()
            diff_blocks += 1

        t1 = time.time()
        if t1 - t_last > 1 or (same_blocks + diff_blocks) >= size_blocks:
            rate = (i + 1.0) * blocksize / (1024.0 * 1024.0) / (t1 - t0)
            print "\rsame: %d, diff: %d, %d/%d, %5.1f MB/s" % (same_blocks, diff_blocks, same_blocks + diff_blocks, size_blocks, rate),
            t_last = t1
 
    print "\n\nCompleted in %d seconds" % (time.time() - t0)
    if showsum:
        print "Source checksum: "+c_sum.hexdigest()
 
    return same_blocks, diff_blocks

def get_hashfunc(hashalg):
    if (hashalg == "md5"):
        hashfunc = hashlib.md5
    elif (hashalg == "sha1"):
        hashfunc = hashlib.sha1
    elif (hashalg == "sha256"):
        hashfunc = hashlib.sha256
    else:
        hashfunc = hashlib.sha512

    return hashfunc
 
if __name__ == "__main__":
    from optparse import OptionParser
    parser = OptionParser(usage="%prog [options] /dev/source user@remotehost [/dev/dest]")
    parser.add_option("-b", "--blocksize", dest="blocksize", action="store", type="int", help="block size (bytes). Default: 1 MiB", default=1024 * 1024)
    parser.add_option("-a", "--hashalg", dest="hashalg", action="store", type="string", help="Hash alg (md5, sha1, sha256, sha512). Default: sha512", default="sha512")
    parser.add_option("-e", "--encalg", dest="encalg", action="store", type="string", help="SSH encryption alg. Default: aes128", default="aes128-cbc")
    parser.add_option("-x", "--nocache", dest="nocache", action="store_true", help="Minimize read cache usage. Default: off. NOTE: it requires the fadvise extension", default=False)
    parser.add_option("-c", "--showsum", dest="showsum", action="store_true", help="Calculate and show complete source hashsum. Default: off", default=False)
    parser.add_option("-C", "--compress", dest="compress", action="store_true", help="Use LZO compression for block transfer. Default: off", default=False)
    parser.add_option("-s", "--sudo", dest="sudo", action="store_true", help="Use sudo. Defaul: off", default=False)
    (options, args) = parser.parse_args()
 
    if len(args) < 2:
        parser.print_help()
        print __doc__
        sys.exit(1)

    hashfunc = get_hashfunc(options.hashalg)
 
    if args[0] == 'server':
        dstdev = args[1]
        server(dstdev, options.blocksize, options.nocache, options.compress)
    else:
        srcdev = args[0]
        dsthost = args[1]
        if len(args) > 2:
            dstdev = args[2]
        else:
            dstdev = srcdev
        sync(srcdev, dsthost, dstdev, options.blocksize, options.hashalg, options.encalg, options.nocache, options.showsum, options.compress, options.sudo)
