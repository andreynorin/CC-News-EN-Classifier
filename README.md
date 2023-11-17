# CC-News-EN Classifier

The code in this project is used to process CC-News-En WARC files, extract news headlines 
from <title> HTML tag, clean the data, and use a Deep Learning classifier to categorize 
the headlines into 8 categories.  This classifier uses labeled training examples from the 
Google News Archive dataset. 

--------------------------------------------
Associated with Baruch College

A ML classifier and a (mostly automated) data processing pipeline I built for a 
Big Data Technologies class in Dec 2022.

The data pipeline is a somewhat involved process of extracting and cleaning headline
data from the Web Archive Files (WARC) used by the Common Crawl open source project. 

An Amazon Linux EC2 runs a Python program that parses out the headline text resulting
in 1000:1 compression. Extracted headlines are cleaned up and stored in Parquet files
which are uploaded to S3. Storing data in Parquet files allows them to be easily loaded
into HDF (Hadoop File System) on the Elastic Map Reduce cluster, or imported into Pandas
dataframes while achieving maximum storage efficiency.

The ML classifier trains a model on the 1.9m labelled examples in the Google News 
dataset (external to Common Crawl) and then applies the model to classify headlines
from the Common Crawl news archive into several categories. This classifier leverages 
DeepLearning (via the BERT framework), and is implemented as a PySpark application 
running in AWS on an EMR (ElasticMapReduce) cluster. 

Classified headlines are written to Parquet files and stored in S3. The trained model
is analyzed for accuracy against a labelled dataset from Google News and per-categor
y results displayed in a confusion matrix. 

WARC file specification:
https://www.loc.gov/preservation/digital/formats/fdd/fdd000236.shtml

Common Crawl News Dataset
https://commoncrawl.org/blog/news-dataset-available

Google News Headlines Dataset
https://sites.google.com/view/headlinedataset/home

Final report for model performance prediction accuracy:
accuracy-metrics.ipynb

Raw (pre-processed) WARC file data sample:
describe-WARC-file-contents.ipynb Jupyter notebook

Key technologies used: Python, Bash, Apache Spark (EMR), BERT NLP, Scikit-Learn
