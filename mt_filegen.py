#!/usr/bin/python
import threading
import getopt
import time
import os
import sys
import Queue
import random
import shutil
from collections import defaultdict
import copy
import math


def get_bytes (size, unit):
    if unit in ('g', 'G'):
        size = size * 1024 * 1024 * 1024
    elif unit in ('t', 'T'):
        size = size * 1024 * 1024 * 1024 * 1024
    else:
        sys.stderr.write("Acceptable units are 'G' or 'T' case insensitive\n")
        sys.exit (1)
    return (size)

def calculate_total_dirs (ctd_depth):
    total = 1
    ct_depth = copy.deepcopy(ctd_depth)
    while len(ct_depth) > 0:
        p = 1;
        for x in range(len(ct_depth)):
            p *= int(ct_depth[x])
        total += p
        ct_depth.pop()
    return (total)

def calc_files_per_dir_bottom (depth, num_files):
    total = 1
    for d in depth:
        total *= int(d)
    ftotal = int(math.ceil(num_files / total))
    return ftotal


def clean_dir (dir_ent):
    dir = dir_ent.keys()[0]
    print "Cleaning " + dir
    shutil.rmtree(dir, ignore_errors=True)
    run_queue.get()
    print "Done cleaning " + dir
    return (0)

def write_files (dir_ent, files_per_dir, ext, file_size):
    dir = dir_ent.keys()[0]
    if dir_ent[dir]:
        print "Writing " + str(files_per_dir) + " files into " + dir
        for x in range(0,files_per_dir):
            clash = True
            while clash:
                fn = random.randint(0, width * files_per_thread * 1000)
                fn = '%x' % fn
                fname = dir + "/" + "file_" + str(fn) + "." + ext
                if not os.path.isfile(fname):
                    clash = False
            with open(fname, "wb") as fout:
                fout.write(os.urandom(file_size))
            if verbose:
                print "\tWrote " + fname
    print "Finished " + dir
    run_queue.get()

def build_dir_list(base, depth, distribution):
    ldepth = copy.deepcopy(depth)
    entry = {}
    try:
        x = ldepth.pop(0)
    except IndexError:
        return(0)
    for d in range(int(x)):
        new_base = base + "/dir" + str(d)
        if (distribution == "mixed") or (distribution == "bottom" and len(ldepth) == 0):
            entry = {new_base : True}
        else:
            entry = {new_base : False}
        dir_queue.put (entry)
        build_dir_list (new_base, ldepth, distribution)


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
    threads = 0
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
        if opt in ('-d', "--depth"):
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
    run_queue = Queue.Queue(maxsize=threads)
    dir_queue = Queue.Queue()
    if not cleanup:
        file_size = int(round(size/num_files))
    else:
        ld = os.listdir(root)
        for w in ld:
            if w != ".snapshot" and w != "~snapshot":
                dname = root + "/" + w
                d_ent = {dname : True}
                dir_queue.put(d_ent)
    depth_save = copy.deepcopy(depth)
    ti = 0
    if not cleanup:
        for w in range (width):
            dir_base = root + "/p" + str(w)
            if (distribution == "mixed" or (distribution == "bottom" and len(dpeth_save) == 0)):
                dir_entry = {dir_base : True}
            else:
                dir_entry = {dir_base : False}
            dir_queue.put(dir_entry)
            build_dir_list (dir_base, depth_save, distribution)
            print dir_queue.queue
        if distribution == "mixed":
            files_per_thread = int(math.ceil(num_files / dir_queue.qsize()))
        elif distribution == "bottom":
            ldepth = copy.deepcopy(depth)
            files_per_thread = calc_files_per_dir_bottom(ldepth, num_files)
    while not dir_queue.empty():
        dir = dir_queue.get()
        dir_name = dir.keys()[0]
        if not cleanup:
            try:
              os.mkdir(dir_name)
            except OSError:
                print "Cleaning " + dir_name
                shutil.rmtree(dir_name)
                os.mkdir(dir_name)
            if threads > 0:
                while run_queue.full():
                    time.sleep(1)
            run_queue.put(dir)
            job.append (threading.Thread(target=write_files, args=(dir,files_per_thread,ext,file_size)))
        else:
            if threads > 0:
                while run_queue.full():
                    time.sleep (1)
            run_queue.put(dir)
            job.append (threading.Thread(target=clean_dir, args=([dir])))
        job[ti].start()
        ti += 1
    if not run_queue.empty():
        print "Waiting for " + str(run_queue.qsize()) + " threads to finish"
        while not run_queue.empty():
            time.sleep (5)
    print "Done"
    sys.exit(0)









