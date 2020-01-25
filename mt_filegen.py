#!/usr/bin/python

from __future__ import print_function
import threading
import getopt
import time
import os
import sys
if int(sys.version[0]) < 3:
    import Queue
else:
    import queue as Queue
import random
import shutil
from collections import defaultdict
import copy
import math

class AtomicCounter:

    def __init__(self, initial=0):
        """Initialize a new atomic counter to given initial value (default 0)."""
        self.value = initial
        self._lock = threading.Lock()


    def increment(self, num=1):
        """Atomically increment the counter by num (default 1) and return the
        new value.
        """
        with self._lock:
            self.value += num
            return self.value



def get_bytes (size, unit):
    if unit in ('g', 'G'):
        size = size * 1024 * 1024 * 1024
    elif unit in ('t', 'T'):
        size = size * 1024 * 1024 * 1024 * 1024
    elif unit in ('m', 'M'):
        size = size * 1024 * 1024
    else:
        sys.stderr.write("Acceptable units are 'M', 'G' or 'T' case insensitive\n")
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

def calc_files_per_dir_bottom (depth, num_files, width):
    total = 1
    for d in depth:
        total *= int(d)
    total *= width
    ftotal = int(math.ceil(num_files / total))
    if ftotal < 1:
        ftotal = 1
    return ftotal

def get_first_dir_entry (dir_queue):
    for x in range (dir_queue.qsize()):
        if dir_queue.queue[x][dir_queue.queue[x].keys()[0]]:
            return dir_queue.queue[x].keys()[0]

def max_files_check (dir_queue, files_per_thread, num_files):
    global file_count
    fcount = file_count.value
    klll_flag = False
    for x in range (dir_queue.qsize()):
        if dir_queue.queue[x][dir_queue.queue[x].keys()[0]]:
            if fcount + files_per_thread > num_files:
                kill_flag = True
                dir_queue.queue[x][dir_queue.queue[x].keys()[0]] = False
            else:
                fcount += files_per_thread
    return (dir_queue)

def clean_dir (dir_ent):
    dir = dir_ent.keys()[0]
    print("Cleaning " + dir)
    shutil.rmtree(dir, ignore_errors=True)
    run_queue.get()
    print("Done cleaning " + dir)
    return (0)

def write_files (dir_ent, files_per_dir, ext, file_size, num_files):
    global file_count
#    dir = dir_ent.keys()[0]
    dir_l = list(dir_ent.keys())
    dir = dir_l[0]
    if dir_ent[dir]:
        print ("Writing " + str(files_per_dir) + " files into " + dir)
        for x in range(0,files_per_dir):
            clash = True
            while clash:
                fn = random.randint(0, width * files_per_thread * 1000)
                fn = '%x' % fn
                fname = os.path.join(dir, 'file_' + str(fn) + "." + ext)
                if not os.path.isfile(fname):
                    clash = False
            bytes_written = 0
            with open(fname, "wb") as fout:
                while bytes_written < file_size:
                    if file_size - bytes_written < WRITE_SIZE:
                        if compressible:
                            c_size = int((file_size - bytes_written) / 2)
                            fout.write(b'\0' * c_size)
                            fout.write(os.urandom(c_size))
                        else:
                            fout.write(os.urandom(file_size - bytes_written))
                        bytes_written += file_size - bytes_written
                    else:
                        if compressible:
                            c_size = int(WRITE_SIZE / 2)
                            fout.write(b'\0' * c_size)
                            fout.write(os.urandom(WRITE_SIZE))
                        else:
                            fout.write(os.urandom(WRITE_SIZE))
                        bytes_written += WRITE_SIZE
                file_count.increment()
            fout.close()
            if verbose:
                print ("\tWrote " + fname + " (" + fcount.value + "/" + num_files)
    run_queue.get()
'''
def build_dir_list(base, depth, distribution):
    ldepth = copy.deepcopy(depth)
    entry = {}
    try:
        x = ldepth.pop(0)
    except IndexError:
        return(0)
    for d in range(int(x)):
        new_base = os.path.jon (base, 'dir' + str(d))
        if (distribution == "mixed") or (distribution == "bottom" and len(ldepth) == 0):
            entry = {new_base : True}
        else:
            entry = {new_base : False}
        dir_queue.put (entry)
        build_dir_list (new_base, ldepth, distribution)
'''
def build_dir_list (plist, depth, distribution):
    ldepth = copy.deepcopy(depth)
    new_plist = []
    entry = {}
    try:
        d = int(ldepth.pop(0))
    except IndexError:
        return (0)
    for x in range(d):
        for p in plist:
            ent = os.path.join(p, 'dir' + str(x))
            new_plist.append(ent)
            if (distribution == "mixed") or (distribution == "bottom" and len(ldepth) == 0):
                entry = {ent : True}
            else:
                entry = {ent : False}
            dir_queue.put (entry)
    build_dir_list (new_plist, ldepth, distribution)




def round_up (root, delta, dir_queue, threads, ext, file_size, rdepth, width, distribution, num_files):
    for w in range(width):
        dir_base = os.path.join(root, 'p' + str(w))
        if (distribution == "mixed" or (distribution == "bottom" and len(depth_save) == 0)):
            dir_entry = {dir_base: True}
            dir_queue.put(dir_entry)
        else:
            dir_entry = {dir_base: False}
            dir_queue.put(dir_entry)
            build_dir_list(dir_base, rdepth, distribution)
    first_dir_entry = get_first_dir_entry (dir_queue)
    if distribution == "mixed":
        files_per_thread = int(math.ceil(delta / dir_queue.qsize()))
    elif distribution == "bottom":
        ldepth = copy.deepcopy(rdepth)
        files_per_thread = calc_files_per_dir_bottom(ldepth, delta, width)
    dir_queue = max_files_check (dir_queue, files_per_thread, num_files)
    mt_writer(dir_queue, cleanup, True, threads, ext, file_size, files_per_thread, num_files)
    print ("First round up done: " + str(file_count.value))
    new_delta = num_files - file_count.value
    if new_delta > 0:
        new_dir_queue = Queue.Queue()
        new_dir_queue.put(first_dir_entry)
        print ("Final round up: " + str(new_delta) + " files")
        mt_writer (dir_queue, cleanup, True, threads, ext, file_size, new_delta)


def mt_writer (dir_queue, cleanup, skip_clean, threads, ext, file_size, files_per_thread, num_files):
    global ti
    while not dir_queue.empty():
        dir = dir_queue.get()
#        dir_name = dir.keys()[0]
        dir_name_l = list(dir.keys())
        dir_name = dir_name_l[0]
        if not cleanup:
            if not skip_clean:
                try:
                    os.mkdir(dir_name)
                except OSError:
                    print("Cleaning " + dir_name)
                    shutil.rmtree(dir_name)
                    os.mkdir(dir_name)
            if threads > 0:
                while run_queue.full():
                    time.sleep(1)
            run_queue.put(dir)
            job.append(threading.Thread(target=write_files, args=(dir, files_per_thread, ext, file_size, num_files)))
        else:
            if threads > 0:
                while run_queue.full():
                    time.sleep(1)
            run_queue.put(dir)
            job.append(threading.Thread(target=clean_dir, args=([dir])))
        job[ti].start()
        ti += 1
    if not run_queue.empty():
        while not run_queue.empty():
            print ("Waiting for " + str(run_queue.qsize()) + " threads to finish")
            time.sleep (5)


def usage():
    sys.stderr.write("Usage: mt_filegen.py [-hvCc] [-d depth] [-s size] [-n number_files] [-e ext] [-t threads] [-D distrubtion] directory\n")
    sys.stderr.write("-h | --help : Prints this message\n")
    sys.stderr.write ("-v | --verbose : Prints each filename as written\n")
    sys.stderr.write("-C | --cleanup : Cleanup files instead of create them\n")
    sys.stderr.write("-c | --compressable : Ensure files are compressable\n")
    sys.stderr.write("-r | --roundup : Make sure the exact number of files is written even if it goes over the size limit\n")
    sys.stderr.write("-d | --depth depth_string : Describes the depth of the tree.  A simple int goes N levels deep.\n")
    sys.stderr.write("    X:Y:Z creates 3 levels deep the first level is X wide, the next level Y wide, then Z wide, etc.\n")
    sys.stderr.write("-s | --size X : Makes the total size of the dataset to X.  Follow X with either M, G or T for Megabutes, Gigabytes or Terrabytes, e.g. 100G or 1T\n")
    sys.stderr.write("-n | --numfiles N : Creates a total of N files\n")
    sys.stderr.write("-e | --ext X : Makes X the extension of the files.  Default is dat\n")
    sys.stderr.write("-t | --threads T : Runs up to T threads.  Each thread works on subdirectory\n")
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
    ti = 0
    num_files = 0
    thread_queue = Queue.Queue()
    job = []
    cjob = []
    distribution = "mixed"
    cleanup = False
    width_flag = False
    verbose = False
    roundup = False
    file_count = AtomicCounter()
    bd_list = []
    WRITE_SIZE = 2097152
    compressible = False

    optlist, args = getopt.getopt(sys.argv[1:], 'hd:e:s:n:t:D:Cvrc', ['help', 'depth=', 'size=', 'numfiles=', 'ext=','threads=', 'distribute=', 'cleanup', 'verbose', 'roundup', 'compressible'])
    for opt, a in optlist:
        if opt in ('-d', "--depth"):
            depth = a.split(':')
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
        if opt in ('-r', "--roundup"):
            roundup = True
        if opt in ('-c', "--compressible"):
            compressible = True
        if opt in ('-h', "--help"):
            usage()
    root = args[0]
    run_queue = Queue.Queue(maxsize=threads)
    dir_queue = Queue.Queue()
    if not cleanup:
        file_size = int(round(size/num_files))
    else:
        ld = os.listdir(root)
        file_size = 0
        files_per_thread = 0
        for w in ld:
            if w != ".snapshot" and w != "~snapshot":
                dname = os.path.join(root, w)
                d_ent = {dname : True}
                dir_queue.put(d_ent)
    width = int(depth.pop(0))
    depth_save = copy.deepcopy(depth)
    ti = 0
    
    if not cleanup:
        for w in range (width):
            dir_base = os.path.join(root, 'p' + str(w))
            if (distribution == "mixed" or (distribution == "bottom" and len(depth_save) == 0)):
                dir_entry = {dir_base : True}
            else:
                dir_entry = {dir_base : False}
            dir_queue.put(dir_entry)
            bd_list.append(dir_base)
        build_dir_list (bd_list, depth_save, distribution)
        if distribution == "mixed":
            files_per_thread = int(math.ceil(num_files / dir_queue.qsize()))
        elif distribution == "bottom":
            ldepth = copy.deepcopy(depth)
            files_per_thread = calc_files_per_dir_bottom(ldepth, num_files, width)
    mt_writer(dir_queue, cleanup, False, threads, ext, file_size, files_per_thread, num_files)
    '''
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
        while not run_queue.empty():
            print "Waiting for " + str(run_queue.qsize()) + " threads to finish"
            time.sleep (5)
    '''
    if roundup:
        delta = num_files - file_count.value
        print("Rounding up " + str(delta) + " files")
        round_up (root, delta, dir_queue, threads, ext, file_size, depth_save, width, distribution,num_files)
    print("Wrote a total of " + str(file_count.value))
    print ("Done")
    sys.exit(0)