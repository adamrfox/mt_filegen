#!/usr/bin/python
import threading
import getopt
import time
import os
import sys
import Queue
import random
import shutil


def get_bytes (size, unit):
    if unit in ('g', 'G'):
        size = size * 1024 * 1024 * 1024
    elif unit in ('t', 'T'):
        size = size * 1024 * 1024 * 1024 * 1024
    else:
        sys.stderr.write("Acceptable units are 'G' or 'T' case insensitive\n")
        sys.exit (1)
    return (size)

def calculate_total_dirs (depth):
    total = 1
    while len(depth) > 0:
        p = 1;
        for x in range(len(depth)):
            p *= int(depth[x])
        total += p
        depth.pop()
    return (total)

def calc_files_per_dir_bottom (depth, files_per_thread):
    total = 1
    for d in depth:
        total *= int(d)
    return total

def build_out (dir, depth, ext, files_per_dir, file_size, distribution):
    ldepth = list(depth)
    if ((distribution == "mixed") or (distribution == "bottom" and len(ldepth) == 0)):
        for x in range (files_per_dir):
            fn = random.randint(0,width*files_per_thread)
            fn = '%x' % fn
            fname = dir + "/" + "file_" + str(fn) + "." + ext
            with open (fname, "wb") as fout:
                fout.write (os.urandom(file_size))
            if verbose:
                print "\tWrote " + fname
    try:
        d = int(ldepth.pop(0))
    except IndexError:
        return(0)
    for x in range (d):
        new_dir = dir + "/dir" + str(x)
        os.mkdir (new_dir)
        build_out (new_dir, ldepth, ext, files_per_dir, file_size, distribution)
    return (0)

def build_dir (root, job_dir, depth, ext, files_per_thread, file_size, distribution):
    print "JOB " + job_dir + " started"
    pdir_root= root + "/" + job_dir
    ldepth = list(depth)
    try:
        os.mkdir (pdir_root)
    except OSError:
        print "Cleaning up " + pdir_root
        shutil.rmtree(pdir_root, ignore_errors=True)
        os.mkdir(pdir_root)
    if len(ldepth) == 1:
        real_depth = int(ldepth[0])
        ldepth = []
        for i in range (real_depth):
            ldepth.append('1')
    total_dirs = calculate_total_dirs (ldepth)
    if distribution == "mixed":
        files_per_dir = int(round(files_per_thread/total_dirs))
    elif distribution == "bottom":
        files_per_dir = calc_files_per_dir_bottom (depth, files_per_thread)
    print "JOB " + job_dir + " writing"
    build_out (pdir_root, depth, ext, files_per_dir, file_size, distribution)
    run_queue.get()
    print "JOB " + job_dir + " done."

def clean_dir (dir):
    print "Cleaning " + dir
    shutil.rmtree(dir, ignore_errors=True)
    run_queue.get()
    print "Done cleaning " + dir
    return (0)

def usage():
    sys.stderr.write("Usage: mt_filegen.py [-hvC] [-d depth] [-s size] [-n number_files] [-e ext] [-t threads] [-D distrubtion] directory\n")
    sys.stderr.write("-h | --help : Prints this message\n")
    sys.stderr.write ("-v | --verbose : Prints each filename as written\n")
    sys.stderr.write("-C | --cleanup : Cleanup files instead of create them\n")
    sys.stderr.write("-d | --depth depth_string : Describes the depth of the tree.  A simple int goes N levels deep.\n")
    sys.stderr.write("    X:Y:Z creates 3 levels deep the first level is X wide, the next level Y wide, then Z wide, etc.\n")
    sys.stderr.write("-s | --size X : Makes the total size of the dataset to X.  Follow X with either G or T for Gigabytes or Terrabytes, e.g. 100G or 1T\n")
    sys.stderr.write("-n | --numfiles N : Creates a total of N files\n")
    sys.stderr.write("-e | --ext X : Makes X the extension of the files.  Default is dat")
    sys.stderr.write("-t | --threads T : Runs up to T threads.  Each thread works on a top level directory (px)\n")
    sys.stderr.write("-D | distrubute mixed|bottom : Set to 'mixed' files are written throutout the tree.  This is the default\n")
    sys.stderr.write("    Set to 'bottom' files are only written at the bottom of the tree\n")
    exit (0)

if __name__ == "__main__":
    ext = "dat"
    threads = 1
    width = 1
    depth = ['1']
    size_s = ''
    size = 0
    num_files = 0
    thread_queue = Queue.Queue()
    ti = 0
    job = []
    cjob = []
    distribution = "mixed"
    cleanup = False
    width_flag = False
    verbose = False

    optlist, args = getopt.getopt(sys.argv[1:], 'hd:w:e:s:n:t:D:Cv', ['help', 'depth=', 'width=', 'size=', 'numfiles=', 'ext=','threads=', 'distribute=', 'cleanup', 'verbose'])
    for opt, a in optlist:
        if opt in ('-d', "--'depth"):
            depth = a.split(':')
        if opt in ('-w', "--width"):
            width_flag = True
            width = int(a)
        if opt in ('-s', "--size"):
            size_s = a
            unit = size_s[-1]
            size = int(size_s[:-1])
            size = get_bytes (size, unit)
        if opt in ('-n', "--numfiles"):
            num_files = int(a)
        if opt in ('-e', "--ext"):
            ext = a
        if opt in ('-t', "--threads"):
            threads = int(a)
        if opt in ('-D', "--distrubute"):
            distribution = a
        if opt in ('-C', "--cleanup"):
            cleanup = True
        if opt in ('-v', "--verbose"):
            verbose = True
        if opt in ('-h', "--help"):
            usage()
    root = args[0]
    global run_queue
    run_queue = Queue.Queue(maxsize=threads)
    if not cleanup:
        files_per_thread = int(num_files/width)
        file_size = int(round(size/num_files))
    if cleanup:
        ld = os.listdir(root)
        for w in ld:
            if w != ".snapshot" and w != "~snapshot":
                thread_queue.put(root + "/" + w)
    else:
        for w in range (width):
            thread_queue.put('p' + str(w))
    while (not thread_queue.empty()):
        while (not run_queue.full() and not thread_queue.empty()):
            job_dir = thread_queue.get()
            if not cleanup:
                orig_depth = list(depth)
                job.append(threading.Thread (target=build_dir, args=(root, job_dir, orig_depth, ext, files_per_thread, file_size, distribution)))
                job[ti].start()
            else:
                cjob.append(threading.Thread(target=clean_dir, args=([job_dir])))
                cjob[ti].start()
            run_queue.put(ti)
            ti += 1
        if not thread_queue.empty():
            time.sleep(5)

