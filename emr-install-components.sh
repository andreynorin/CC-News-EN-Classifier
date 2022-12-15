#!/bin/bash
set -x -e

echo -e 'export PYSPARK_PYTHON=/usr/bin/python3
export HADOOP_CONF_DIR=/etc/hadoop/conf
export SPARK_JARS_DIR=/usr/lib/spark/jars
export SPARK_HOME=/usr/lib/spark' >> $HOME/.bashrc && source $HOME/.bashrc

sudo python3 -m pip install awscli boto spark-nlp sklearn spark-sklearn

yes | sudo yum install git

cd /tmp
git clone "https://github.com/andreynorin/BigDataAnalytics.git"

set +x
exit 0