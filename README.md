# mt_filegen
A project to build a multi-threaded file generation tool

This is a tool that generates a set of files in a given tree.  It allows the user to set the depth as well as width of the tree at each level, the number of file, the total size of the dataset, as well as whether the data is compressible or not.
Working in storage and data management for years, I have found it useful to be able to generate dummy datasets that mimic
certain applicaions or customer environments.  I never really liked the tools that were out there although some are certainly
better than others.  And I wanted an excuse to learn multi-threading in Python, so mt_filegen was born.

It currently is set to run with Python 2.7 or 3.x.  If you find code compatibility issues, let me know or feel free to contribute.

Let's start with the syntax:
<pre>
Usage: mt_filegen.py [-hvCcr] [-d depth] [[-s size] | [-f size]] [-n number_files] [-e ext] [-t threads] [-D distribution] directory
-h | --help : Prints this message
-v | --verbose : Prints each filename as written
-C | --cleanup : Cleanup files instead of create them
-c | --compressible : Ensure files are compressible
-r | --roundup : Make sure the exact number of files is written even if it goes over the size limit
-d | --depth depth_string : Describes the depth of the tree.  A simple int goes N levels deep.
    X:Y:Z creates 3 levels deep the first level is X wide, the next level Y wide, then Z wide, etc.
-s | --size=X : Makes the total size of the dataset to X.  Follow X with either K, M, G or T for Kilobytes, Megabutes, Gigabytes or Terrabytes, e.g. 100G or 1T
-f | --filesize= X[:Y] Makes the file size of each file between X an Y. Follow X/Y with either K, M, G or T for Kilobytes, Megabutes, Gigabytes or Terrabytes, e.g. 100G or 1T
-S | --sparse | Makes the files sparse files.
-n | --numfiles N : Creates a total of N files
-e | --ext X : Makes X the extension of the files.  Default is dat
-t | --threads T : Runs up to T threads.  Each thread works on subdirectory
-D | distrubute mixed|bottom : Set to 'mixed' files are written throutout the tree.  This is the default
    Set to 'bottom' files are only written at the bottom of the tree
</pre>
Hopefully this explains most of it.  Here is a common examples:

mt_filegen.py -d 2:3:5:6:10 -n 1000000 -s 100G -t 5 /mypath

This will create 5 levels of a tree.  The first level will have 2 directories, the next 3, the next 5, etc.  There will be 1M files and the total size will be 100GB. The files will be distributed throghout the directories in a somewhat even fashion.  And it will run 5 threads at a time.  
NOTE:  In these cases there are usually instnaces of rounding.  This syntax prefers the size over the number of files.  If the number of files is more important, specify the -r flag and it will allow the size limit to be exceeded to get the number of files exactly as specified.
