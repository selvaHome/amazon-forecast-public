import boto3
from time import sleep
import subprocess
import pandas as pd
from utils import create_iam_policy

# create a session
session = boto3.Session()
forecast = session.client(service_name='forecast')
forecastquery = session.client(service_name='forecastquery')


# specify bucket name and key
accountId = boto3.client('sts').get_caller_identity().get('Account')
bucketName = "bucket-for-amazon-forecast-selva".format(accountId)
key = "elec_data/item-demand-time.csv"


# create the bucket if doesn't exist already
s3 = session.client('s3')
s3.create_bucket(Bucket=bucketName)
s3.upload_file(Filename="data/item-demand-time.csv", Bucket=bucketName, Key=key)


# to create IAM policy for forecast - one time thing
roleArn = awsutils.create_iam_policy()


# spcify data necessary for create_dataset, create_dataset_group & create_dataset_import_job
DATASET_FREQUENCY = "H"
TIMESTAMP_FORMAT = "yyyy-MM-dd hh:mm:ss"
project = 'project4selva'
datasetName= project+'_ds'
datasetGroupName= project +'_gp'
s3DataPath = "s3://"+bucketName+"/"+key


# Specify the schema of your dataset here. Make sure the order of columns matches the raw data files.
schema ={
   "Attributes":[
      {
         "AttributeName":"timestamp",
         "AttributeType":"timestamp"
      },
      {
         "AttributeName":"target_value",
         "AttributeType":"float"
      },
      {
         "AttributeName":"item_id",
         "AttributeType":"string"
      }
   ]
}


# create a new dataset
response_dataset=forecast.create_dataset(
                    Domain="CUSTOM",
                    DatasetType='TARGET_TIME_SERIES',
                    DatasetName=datasetName,
                    DataFrequency=DATASET_FREQUENCY,
                    Schema = schema
                   )
datasetArn = response_dataset['DatasetArn']
print("datasetArn :", datasetArn)


# create a new datasetgroup
create_dataset_group_response = forecast.create_dataset_group(DatasetGroupName=datasetGroupName,
                                                                  Domain="CUSTOM",
                                                                  DatasetArns=[datasetArn]
                                                                  )
datasetGroupArn = create_dataset_group_response['DatasetGroupArn']
print("datasetGroupArn:", datasetGroupArn)


# import dataset
datasetImportJobName = 'EP_AML_DSIMPORT_JOB_TARGET'
ds_import_job_response = forecast.create_dataset_import_job(DatasetImportJobName=datasetImportJobName,
                                                                DatasetArn=datasetArn,
                                                                DataSource={
                                                                    "S3Config": {
                                                                        "Path": s3DataPath,
                                                                        "RoleArn": roleArn
                                                                    }
                                                                },
                                                                TimestampFormat=TIMESTAMP_FORMAT
                                                                )
ds_import_job_arn = ds_import_job_response['DatasetImportJobArn']
while True:
    dataImportStatus = forecast.describe_dataset_import_job(DatasetImportJobArn=ds_import_job_arn)['Status']
    print(dataImportStatus)
    if dataImportStatus != 'ACTIVE' and dataImportStatus != 'CREATE_FAILED':
        sleep(30)
    else:
        break
print("ds_import_job_arn :", ds_import_job_arn)


# create predictor
predictorName = project+'_autoML'
forecastHorizon = 24
create_predictor_response = forecast.create_predictor(PredictorName=predictorName,
                                                          ForecastHorizon=forecastHorizon,
                                                          PerformAutoML=True,
                                                          PerformHPO=False,
                                                          EvaluationParameters={"NumberOfBacktestWindows": 1,
                                                                                "BackTestWindowOffset": 24},
                                                          InputDataConfig={"DatasetGroupArn": datasetGroupArn},
                                                          FeaturizationConfig={"ForecastFrequency": "H",
                                                                               "Featurizations":
                                                                                   [
                                                                                       {"AttributeName": "target_value",
                                                                                        "FeaturizationPipeline":
                                                                                            [
                                                                                                {
                                                                                                    "FeaturizationMethodName": "filling",
                                                                                                    "FeaturizationMethodParameters":
                                                                                                        {
                                                                                                            "frontfill": "none",
                                                                                                            "middlefill": "zero",
                                                                                                            "backfill": "zero"}
                                                                                                    }
                                                                                            ]
                                                                                        }
                                                                                   ]
                                                                               }
                                                          )

predictorArn = create_predictor_response['PredictorArn']

while True:
    predictorStatus = forecast.describe_predictor(PredictorArn=predictorArn)['Status']
    print(predictorStatus)
    if predictorStatus != 'ACTIVE' and predictorStatus != 'CREATE_FAILED':
        sleep(30)
    else:
        break
print("predictorArn :", predictorArn)


# get model accuracy
accuracy_response = forecast.get_accuracy_metrics(PredictorArn=predictorArn)
print("model accuracy :", accuracy_response)


# create forecaster
forecastName= project+'_aml_forecast'
create_forecast_response = forecast.create_forecast(ForecastName=forecastName,
                                                        PredictorArn=predictorArn)
forecastArn = create_forecast_response['ForecastArn']

while True:
    forecastStatus = forecast.describe_forecast(ForecastArn=forecastArn)['Status']
    print(forecastStatus)
    if forecastStatus != 'ACTIVE' and forecastStatus != 'CREATE_FAILED':
        sleep(30)
    else:
        break
print("forecastArn :", forecastArn)



# get predictions
forecastResponse = forecastquery.query_forecast(
    ForecastArn=forecastArn,
   Filters={"item_id":"client_12"}
)
print(forecastResponse)


# export the forecast to S3 bucket
forecastExportName= project+'_aml_forecast_export'
outputPath = "s3://" + bucketName + "/output"
forecast_export_response = forecast.create_forecast_export_job(
        ForecastExportJobName=forecastExportName,
        ForecastArn=forecastArn,
        Destination={
            "S3Config": {
                "Path": outputPath,
                "RoleArn": roleArn
            }
        }
    )

forecastExportJobArn = forecast_export_response['ForecastExportJobArn']
while True:
    forecastExportStatus = forecast.describe_forecast_export_job(ForecastExportJobArn=forecastExportJobArn)['Status']
    print(forecastExportStatus)
    if forecastExportStatus != 'ACTIVE' and forecastExportStatus != 'CREATE_FAILED':
        sleep(30)
    else:
        break
print("forecastExportJobArn :", forecastExportJobArn)


# Delete the resources to avoid any additional cost
# Delete Import
forecast.delete_dataset_import_job(DatasetImportJobArn=ds_import_job_arn)
# Delete Dataset Group
forecast.delete_dataset_group(DatasetGroupArn=datasetGroupArn)
# Delete predictor
forecast.delete_predictor(PredictorArn=predictorArn)