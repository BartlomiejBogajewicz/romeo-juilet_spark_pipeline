In this project I have created a basic airflow data pipeline to practise pyspark in which im counting words in Romeo and juliet book and store that information in hive table. We can divide this project into three pieces
 
1. saving_rates - it's bash operator that move input file in docker container to hdfs file system
2. create_romeo_table - create hive table where we will store spark job output
3. spark_job - here we are counting words in romeo and juliet book and store only those which appeared more than sixty times  

Below you can see the pipeline in airflow and hive table displayed in hue

![airflow pipeline](https://github.com/BartlomiejBogajewicz/romeo_juilet_spark_pipeline/blob/main/airflow_dags.PNG)

![hive table](https://github.com/BartlomiejBogajewicz/romeo_juilet_spark_pipeline/blob/main/hive_table.PNG)

