#!/usr/bin/env python3

# Author:       Andrey Norin
# Title :       Process WARC - Extract page titles and save to Parquet files
# Date Created: 11/12/2022
# Date Updated: 11/26/2022
# Update log: - fixed memory leak caused by beautiful soup with soup.decompose() and convert title to str()

# take inputDir and outputDir as params

import sys
import os
import glob
import subprocess
import json
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from numpy import int64
from warcio.archiveiterator import ArchiveIterator

# parse passed in args
inputDir  = sys.argv[1]
outputDir = sys.argv[2]

#inputDir = 'c:/Users/ANDRE/OneDrive/Desktop/CodeTest'
#outputDir =  'c:/Users/ANDRE/OneDrive/Desktop/CodeTest'


# change pwd
print("script called with inputDir:", inputDir)
print("script called with outputDir:", outputDir)


# change pwd to inputDir and enumerate files into a list
os.chdir(inputDir)
path = os.getcwd()
print("changed path to:",path)

# add all WARC files to inputFiles list
inputFiles = glob.glob('*warc*')


def build_titles_df():

    with open(fileName, 'rb') as stream:

        print("Generating dataframe")
        titles = []

        recordCounter = 0
        for record in ArchiveIterator(stream):
                if record.rec_type == 'response':
                    payload_content = record.raw_stream.read()
                    soup             = BeautifulSoup(payload_content, 'html.parser')
                    if (soup.title is not None):
                        title = str(soup.title.string)
                        #df.loc[recordCounter] = [title]

                        #print(title)
                        titles += [title]
                    soup.decompose()
                recordCounter += 1

        df = pd.DataFrame(columns=(['Title']))
        df = pd.DataFrame(columns=(['Title']), data=titles)

        df.head()
        df.to_parquet(parquetFilePath)
        del df

        # delete processed file
        print("Removing processed file:",fileName)
        cmd = ['rm',fileName]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        o, e = proc.communicate()

        print('Output: ' + o.decode('ascii'))
        print('Error: '  + e.decode('ascii'))
        print('code: ' + str(proc.returncode))


# process files in the list
for fileName in inputFiles:

    os.chdir(inputDir)
    fileNameNoExt       = fileName.split('.')[0]
    parquetFileName     = fileNameNoExt +'.parquet'
    parquetFilePath     = os.path.join(path, outputDir, parquetFileName)

    print("Processing file:",fileName)
    print("Output file name will be:", parquetFilePath)
    build_titles_df()

print("Processing complete.")