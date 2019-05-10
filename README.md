### Loading Json data by creating dynamic BQ table and schema 

Create a template:

gradle run -DmainClass=com.google.swarm.datagenerator.BQStream -Pargs=" --streaming --project=<id> --runner=DataflowRunner --tempLocation=gs://df-template-temp/temp --templateLocation=gs://df-template-temp/mutation_template --dataSetId=<id> --subTopic=projects/<id>/subscriptions/df-sub --numWorkers=115 --workerMachineType=n1-standard-8 --maxNumWorkers=115 --autoscalingAlgorithm=NONE --experiments=shuffle_mode=service"



### Run the template:
gcloud dataflow jobs run data-pipeline --gcs-location gs://df-template-temp/mutation_template



### Mock Data Generator from Pub/Sub to BQ in JSON
gradle run -DmainClass=com.google.swarm.datagenerator.StreamingBenchmark -Pargs="--streaming --project=<id> --runner=DataflowRunner  --numWorkers=20 --maxNumWorkers=20 --workerMachineType=n1-standard-4 --qps=200000 --schemaLocation=gs://input-json/locations.json --eventType=locations --topic=projects/<id>/topics/events --templateLocation=gs://df-template-temp/locations --experiments=shuffle_mode=service  --autoscalingAlgorithm=NONE"

gcloud dataflow jobs run location-pipeline --gcs-location gs://df-template-temp/locations

#### Alternative Option By passing Pub/Sub 
gradle run -Pargs=" --streaming --project=<id> --batchSize=1000 --numberofRows=10000 --tableSpec=bq-sql-load-test:telco_analytics.clickstream_raw --inputFile=gs://bq-schema-files/clickstream_schema.json"