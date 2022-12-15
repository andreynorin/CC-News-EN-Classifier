#!/usr/bin/env python3

# take inputDir and outputDir as params

import sys
import os
import glob
import subprocess
import json
import numpy as np
import pandas as pd

# parse passed in args
inputDir  = sys.argv[1]
outputDir = sys.argv[2]

# change pwd
print("script called with inputDir:", inputDir)
print("script called with outputDir:", outputDir)


# change pwd to inputDir and enumerate files into a list
os.chdir(inputDir)
path = os.getcwd()
print("changed path to:",path)

# add all WARC files to inputFiles list
inputFiles = glob.glob('*warc*')

# process files in the list
for fileName in inputFiles:

    os.chdir(inputDir)
    fileNameNoExt  = fileName.split('.')[0]
    newFileName    = fileNameNoExt +'.recomp.warc.gz'
    newFilePath    = os.path.join(path, outputDir, newFileName)

    print("Recompressing",fileName,"to",newFilePath)
    cmd = ['warcio','recompress', fileName, newFilePath]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    o, e = proc.communicate()

    print('Output: ' + o.decode('ascii'))
    print('Error: '  + e.decode('ascii'))
    print('code: ' + str(proc.returncode))

    # delete old file
    print("Removing processed file:",fileName)
    cmd = ['rm',fileName]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    o, e = proc.communicate()

    print('Output: ' + o.decode('ascii'))
    print('Error: '  + e.decode('ascii'))
    print('code: ' + str(proc.returncode))

print("Processing complete.")