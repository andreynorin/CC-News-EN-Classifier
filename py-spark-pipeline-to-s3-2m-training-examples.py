# my-job.py
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("PySparkPipelineToS3.py").getOrCreate()
sc = spark.sparkContext

# the following code is run in the PySpark prompt

from sparknlp.base import *
from sparknlp.annotator import *
from sparknlp.pretrained import PretrainedPipeline
import sparknlp
from pyspark.ml import Pipeline
import pyspark.sql.functions as f
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit

# this is for analyzing full datset
#tempDF1=spark.read.parquet("hdfs:///GoogleTrainingData/*.parquet")

#this parquet file has 388014 records
#tempDF1=spark.read.parquet("hdfs:///GoogleTrainingData/tcilen6wu42p3p6m3q5n22m53u.parquet")

# this parquet file has 1979830 records
tempDF1=spark.read.parquet("hdfs:///GoogleTrainingData/bhbkh2jowm4ztp4bckxxuwsuje.parquet")
tempDF1.count()

# this loads full available headlines dataset
tempDF2=spark.read.parquet("hdfs:///CC-News-En-Titles-Clean-Data//*.parquet")

# this file has 9194 records with date field added
#tempDF2=spark.read.parquet("hdfs:///CC-News-En-Titles-Clean-Data//CC-NEWS-20160826132734-00001_ENG.clean.parquet")
tempDF2.count()

tempDF1 = tempDF1.withColumn('index', f.monotonically_increasing_id())

trainDataset    = tempDF1
predictDataset  = tempDF2.withColumn("topic", lit("some topic")).select(["topic","date","title"])

# splitting the dataset int 1.8m and 0.1m records for train/test
trainDataset = tempDF1.sort('index').limit(1800000)
testDataset = tempDF1.sort('index', ascending = False).limit(100000)

trainDataset.groupBy("topic").count().orderBy(col("count").desc()).show()
testDataset.groupBy("topic").count().orderBy(col("count").desc()).show()


document_assembler = DocumentAssembler().setInputCol("title") .setOutputCol("document")
tokenizer          = Tokenizer().setInputCols(["document"]) .setOutputCol("token")
bert_embeddings    = BertEmbeddings().pretrained(name='small_bert_L4_256', lang='en') .setInputCols(["document",'token']).setOutputCol("embeddings")
embeddingsSentence = SentenceEmbeddings().setInputCols(["document", "embeddings"]) .setOutputCol("sentence_embeddings") .setPoolingStrategy("AVERAGE")
classsifierdl      = ClassifierDLApproach().setInputCols(["sentence_embeddings"]).setOutputCol("class").setLabelColumn("topic").setMaxEpochs(10).setLr(0.001).setBatchSize(8).setEnableOutputLogs(True)#.setOutputLogsPath('logs')

bert_clf_pipeline  = Pipeline(stages=[document_assembler,tokenizer,bert_embeddings,embeddingsSentence,classsifierdl])

# training the model - this may take a fairly long time
# for 5 files start time - 10:14am, end time -  10:22am
bert_clf_pipelineModel = bert_clf_pipeline.fit(trainDataset)

# make sanity check predictions
preds = bert_clf_pipelineModel.transform(testDataset)
testDataset_preds_df = preds.select('topic','title','class.result')
testDataset_preds_df.count()
testDataset_preds_df.show(20)

# export sanity check predictions to S3
testDataset_preds_df.write.format('parquet').option('header','true').save("s3a://litter-box/fromHDFS/2m-dataset-predictions.parquet", mode='overwrite')

# make predictions against CC-News-En dataset
preds = bert_clf_pipelineModel.transform(predictDataset)
preds_df = preds.select('topic','date','title','class.result')
preds_df.count()
preds_df.show(20)

# export predictions to S3
preds_df.write.format('parquet').option('header','true').save("s3a://litter-box/fromHDFS/2m-cc-news-en-clean-predictions.parquet", mode='overwrite')