# Running Spark in the Cloud

## Connecting to Google Cloud Storage 

Uploading data to GCS:

```bash
gsutil -m cp -r pq/ gs://dtc_data_lake_de-zoomcamp-nytaxi/pq
```

Download the jar for connecting to GCS to any location (e.g. the `lib` folder):

**Note**: For other versions of GCS connector for Hadoop see [Cloud Storage connector ](https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage#connector-setup-on-non-dataproc-clusters).

```bash
gsutil cp gs://hadoop-lib/gcs/gcs-connector-hadoop3-2.2.5.jar ./lib/
```

See the notebook with configuration in [09_spark_gcs.ipynb](09_spark_gcs.ipynb)

(Thanks Alvin Do for the instructions!)


## Local Cluster and Spark-Submit

### Master
#### For windows:
The launch scripts located at %SPARK_HOME%\sbin do not support Windows. You need to manually run the master and worker as outlined below.

Go to %SPARK_HOME%\bin folder in a command prompt

Run spark-class org.apache.spark.deploy.master.Master to run the master. This will give you a URL of the form spark://ip:port

Run spark-class org.apache.spark.deploy.worker.Worker spark://ip:port to run the worker. Make sure you use the URL you obtained in step 2.

Run spark-shell --master spark://ip:port to connect an application to the newly created cluster.


#### For linux:

Creating a stand-alone cluster ([docs](https://spark.apache.org/docs/latest/spark-standalone.html)):

```bash
./sbin/start-master.sh
```

### Workers
Now that we have a master, it also needs workers, if you don't have any workers you can't execute any jobs
Otherwise you'll get something like:
01/01/99 00:00:52 WARN Master: App app-20250523000052-0000 requires more resource than any of Workers could have.
#### Windows
Run spark-class org.apache.spark.deploy.worker.Worker spark://ip:port to run the worker. Make sure you use the URL you obtained in step 2.

#### Linux
Creating a worker:

```bash
URL="spark://de-zoomcamp.europe-west1-b.c.de-zoomcamp-nytaxi.internal:7077"
./sbin/start-slave.sh ${URL}

# for newer versions of spark use that:
#./sbin/start-worker.sh ${URL}
```

### Turn the notebook into a script:

#### Or on Windows
In vscode just click on ... export as python script

#### Linux
```bash
jupyter nbconvert --to=script 06_spark_sql.ipynb
```

#### Then
Edit the script and then run it:

```bash 
python 06_spark_sql.py \
    --input_green=data/pq/green/2020/*/ \
    --input_yellow=data/pq/yellow/2020/*/ \
    --output=data/report-2020
```
Or in my case
```bash 
python Cloud-book.py \
--input_green=../../Data/data/csv/green/spark_parquet/*/* \
--input_yellow=../../Data/data/csv/yellow/spark_parquet/*/* \
--output=../../Data/data/module-05-batch/cloud-book/report/revenue
```
Or oneliner
```bash 
python Cloud-book.py --input_green=../../Data/data/csv/green/spark_parquet/*/* --input_yellow=../../Data/data/csv/yellow/spark_parquet/*/* --output=../../Data/data/module-05-batch/cloud-book/report/revenue
```

### Spark Submit
#### Linux
Use `spark-submit` for running the script on the cluster, 

```bash
URL="spark://de-zoomcamp.europe-west1-b.c.de-zoomcamp-nytaxi.internal:7077"

spark-submit \
    --master="${URL}" \
    06_spark_sql.py \
        --input_green=data/pq/green/2021/*/ \
        --input_yellow=data/pq/yellow/2021/*/ \
        --output=data/report-2021
```

#### Windows
Replace cloud-book.py by the full path if running in windows from spark bin folder, and also the full path to the input and output folders, and also give the right python interpreter
```bash
spark-submit --master spark://192.168.0.181:7077 --conf spark.pyspark.python="C:/Sandeep SSD/Programming SSD/Data Engineering Zoomcamp/data-engineering-zoomcamp/dataenginzoomvenv/Scripts/python.exe" "C:\Sandeep SSD\Programming SSD\Data Engineering Zoomcamp\data-engineering-zoomcamp\05-batch\code\Cloud-book.py" --input_green="C:/Sandeep SSD/Programming SSD/Data Engineering Zoomcamp/data-engineering-zoomcamp/Data/data/csv/green/spark_parquet/*/*" --input_yellow="C:/Sandeep SSD/Programming SSD/Data Engineering Zoomcamp/data-engineering-zoomcamp/Data/data/csv/yellow/spark_parquet/*/*" --output="C:/Sandeep SSD/Programming SSD/Data Engineering Zoomcamp/data-engineering-zoomcamp/Data/data/module-05-batch/cloud-book/report/revenue"
```

### Data Proc

#### Creating a Data Cluster
On GCP go to Data Proc and create a new cluster
Check your usage quota's in your VM's, like with CPUS_PER_VM_FAMILY, to see what computes you can use

#### Uploading the script file to GCP
Upload the script to GCS:
Use whatever you called yours, mine is Cloud-book.py
```bash
gsutil -m cp -r 06_spark_sql.py gs://dtc_data_lake_de-zoomcamp-nytaxi/code/06_spark_sql.py
```

#### Using the script by submitting the arguments too
Params for the job, replace them with your correct GCP uris, copy paste them one by one into the arguments and click enter each time:
* `--input_green=gs://dtc_data_lake_de-zoomcamp-nytaxi/pq/green/2021/*/`
* `--input_yellow=gs://dtc_data_lake_de-zoomcamp-nytaxi/pq/yellow/2021/*/`
* `--output=gs://dtc_data_lake_de-zoomcamp-nytaxi/report-2021`

If you run out of memory you can add these Spark properties to the job as key and value:
Property      Value
spark.driver.memory 4g
spark.executor.instances    1
spark.executor.memory   4g
spark.sql.shuffle.partitions    50

#### Spark History
Under cluster details - web interfaces you can go to spark history server and see all the jobs there


#### Using commandline and gcloud to submit it
Using Google Cloud SDK for submitting to dataproc
([link](https://cloud.google.com/dataproc/docs/guides/submit-job#dataproc-submit-job-gcloud))

```bash
gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=europe-west6 \
    gs://dtc_data_lake_de-zoomcamp-nytaxi/code/06_spark_sql.py \
    -- \
        --input_green=gs://dtc_data_lake_de-zoomcamp-nytaxi/pq/green/2020/*/ \
        --input_yellow=gs://dtc_data_lake_de-zoomcamp-nytaxi/pq/yellow/2020/*/ \
        --output=gs://dtc_data_lake_de-zoomcamp-nytaxi/report-2020
```

### Connecting Spark to Big Query

#### Upload the script to GCS:

```bash
gsutil -m cp -r 06_spark_sql_big_query.py gs://dtc_data_lake_de-zoomcamp-nytaxi/code/06_spark_sql_big_query.py
```

#### Write results to big query ([docs](https://cloud.google.com/dataproc/docs/tutorials/bigquery-connector-spark-example#pyspark)):
https://cloud.google.com/dataproc/docs/tutorials/bigquery-connector-spark-example#pyspark

Structure of the command:
For dataproc clusters mad with image version 2.1 or later they don't need jar argument, they have a jar connector already installed, so no problem, see note below
```bash
gcloud dataproc jobs submit pyspark \
    --cluster=name-of-cluster \
    --region=name-region \
    --properties=property.one=value, property.two=value \
    gs://location/to/your/script \
    -- \
        --input_green=gs://location/to/your/green/files \
        --input_yellow=gs://location/to/yellow/files \
        --output=projectid.schema_or_dataset_name.table_you_want_to_write_to
```
The "table_you_want_to_write_to" doesn't have to exist, gcloud will make it itself

Filed out script with random values:
```bash
gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=europe-west6 \
    --properties=spark.driver.memory=4g,spark.executor.instances=1,spark.executor.memory=4g,spark.sql.shuffle.partitions=50 \
    gs://dtc_data_lake_de-zoomcamp-nytaxi/code/06_spark_sql_big_query.py \
    -- \
        --input_green=gs://dtc_data_lake_de-zoomcamp-nytaxi/pq/green/2020/*/ \
        --input_yellow=gs://dtc_data_lake_de-zoomcamp-nytaxi/pq/yellow/2020/*/ \
        --output=de-zoomcamp-course-65653.module_5.reports-2020
```

One with the jar just in case:
```bash
gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=europe-west6 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    gs://dtc_data_lake_de-zoomcamp-nytaxi/code/06_spark_sql_big_query.py \
    -- \
        --input_green=gs://dtc_data_lake_de-zoomcamp-nytaxi/pq/green/2020/*/ \
        --input_yellow=gs://dtc_data_lake_de-zoomcamp-nytaxi/pq/yellow/2020/*/ \
        --output=de-zoomcamp-course-65653.module_5.reports-2020
```

#### Explain
What this does is it will run submit a job to the data proc cluster to run your script with pyspark, with those input arguments, and your dataproc spark configured with those properties
It'll read the data from GCS storage, transform it and do calculations, and in the end it'll write them back out in BigQuery, to the dataset and table you specified


There can be issue with latest Spark version and the Big query connector. Download links to the jar file for respective Spark versions can be found at:
[Spark and Big query connector](https://github.com/GoogleCloudDataproc/spark-bigquery-connector)

**Note**: Dataproc on GCE 2.1+ images pre-install Spark BigQquery connector: [DataProc Release 2.2](https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-release-2.2). Therefore, no need to include the jar file in the job submission.