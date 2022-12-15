#!/usr/bin/env python3

# take inputDir and outputDir as params

import sys
import os
import glob
import subprocess
import json
import numpy as np
import pandas as pd
from datetime import datetime

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

# add all Parquet files to inputFiles list
inputFiles = glob.glob('*.parquet')

# process files in the list
for fileName in inputFiles:

    os.chdir(inputDir)
    fileNameNoExt  = fileName.split('.')[0]
    newFileName    = fileNameNoExt +'.clean.parquet'
    newFilePath    = os.path.join(path, outputDir, newFileName)

    print('Processing', fileName)
    df = pd.read_parquet(fileName)

    # drop duplicates
    df = df.drop_duplicates()

    # remove all line breaks
    df = df.replace(r'\n',' ', regex=True)

    # remove all tab characters
    df = df.replace(r'\t',' ', regex=True)

    # remove all extra spaces
    df = df.replace(r'\s+',' ', regex=True)

    # dropping empty columns
    df = df.dropna()

    # publication info typically follows a | character, remove it with a lambda function
    df['Title'] = df['Title'].apply(lambda x: x.split('|')[0])

    # more publication info is typically separated via space-hyphen-space combination
    df['Title'] = df['Title'].apply(lambda x: x.split(' - ')[0])

    # drop nulls
    df = df[~df['Title'].isnull()] 

    # get rid of lines with non english characters (non alphanum)
    df = df[df['Title'].map(lambda x: x.isascii())]

    # drop duplicates
    df = df.drop_duplicates()

    # get rid of special cases
    df['Title'] = df.replace(r'(page 1)','', regex=True)
    df['Title'] = df.replace(r'\(*\)','', regex=True)
    df['Title'] = df.replace(r'\(','', regex=True)

    # reset index
    df = df.reset_index(drop=True)

    # add date column
    dateStr    = fileName.split(sep='-')[2][:8]
    fileDate   = (datetime.strptime(dateStr, '%Y%m%d'))
    df['date'] = fileDate

    # write output file
    df.to_parquet(newFilePath)


print("Processing complete.")