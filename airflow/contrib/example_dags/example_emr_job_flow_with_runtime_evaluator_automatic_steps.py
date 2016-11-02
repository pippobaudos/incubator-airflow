# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.emr_create_job_flow_operator_with_runtime_evaluator import EmrSpotAirflowException, \
    EmrCreateJobFlowOperatorWithRuntimeEvaluator
from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor

DEFAULT_ARGS = {
    'owner': 'wikido',
    'depends_on_past': False,
    'start_date': datetime(2016, 3, 13),
    'email': ['niccolo.becchi@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True
}

WIKIDO_CRAWLER_STEPS = [
    {
        "HadoopJarStep": {
            "MainClass": "it.wikido.crawler.nutch.img_size_extractor.ImgSizeCapture_MainMR",
            "Args": [
                "-pathconfig", "s3n://wikido-test/conf/",
                "-pathdistributedcache", "s3n://wikido-test/all_usa_states/cache/",
                "-pathCrawlDb0_In", "s3n://wikido-test/backup_crawldb/all_usa_states_v_XXXXXX/",
                "-verifyOfficialLinks",
                "-doClusterEvents",
                "-pushUriXmlSolrImportToS3ForCrontabImport"
            ],
            "Jar": "s3n://wikido-test/wikido-1.3.14.job"
        },
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "Name": "fase_2_RecuperoImgAndResizeAndCluster"
    },
    {
        "HadoopJarStep": {
            "MainClass": "it.wikido.crawler.nutch.WikiDoMR",
            "Args": [
                "-numloops", "20",
                "-pathconfig", "s3n://wikido-test/conf/",
                "-pathcrawldb", "all_usa_states_v_XXXXXX/",
                "-pathseedfile", "s3n://wikido-test/all_usa_states/seed/seed_all_usa_states_2014_04_plus_bing_2016_04.txt",
                "-pathdistributedcache", "s3n://wikido-test/all_usa_states/cache/",
                "-awsResizeTaskGroup_InstanceCount", "0",
                "-awsResizeTaskGroup_AfterNumloops", "5"
            ],
            "Jar": "s3n://wikido-test/wikido-1.3.14.job"
        },
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "Name": "fase_1_RecuperoEventi"
    }
]

JOB_FLOW_OVERRIDES = {
    'Name': 'WikiDo_Crawler',
    'Steps': WIKIDO_CRAWLER_STEPS
}


def job_flow_overrides_runtime_method():
    #WIKIDO_HOME =$1
    WIKIDO_VERSION = '1.3.14'
    NUM_PASSI_CRAWLING = 20
    EC2_AMI_VERSION = '2.4.11'
    EC2_MASTER_INSTANCE_TYPE = 'm1.large'
    EC2_KEY_PAIR_FILE = 'pippo3'
    EC2_MAX_SLAVE_BID_PRICE_ON_STARTING = 1.1
    EC2_MAX_CORE_BID_PRICE_ON_RUNNING = 3.01
    EC2_MAX_TASK_BID_PRICE_ON_RUNNING = 2.10

    S3_SEED_FILE ='s3n://wikido - test/all_usa_states/seed/seed_all_usa_states_2014_04_plus_bing_2016_04.txt'

    # # ## Call Script to Evaluate current Spot prices for Workers
    # # cd $WIKIDO_HOME / src / bash - script / elastic - map - reduce - jobs / launch /
    # #
    # # eval $(. / check - spot - price - metrics.sh)
    # # ## Override Parameters check cheapest zone disabled
    # # # EC2_BEST_AVAILABILITY_ZONE=us-east-1a
    # # # EC2_BEST_SLAVE_INSTANCE_TYPE=c3.8xlarge
    # # # EC2_BEST_SLAVE_SPOT_PRICE=0.458900
    # # # EC2_MASTER_INSTANCE_TYPE=m4.large
    #
    #
    # now =$(date + "%m_%d_%Y-%H:%M:%S")

    ## Override Parameters on forced relaunch
    # TODO Add here logic to invoke computation of best price
    EC2_BEST_AVAILABILITY_ZONE='us-east-1a'
    EC2_BEST_SLAVE_INSTANCE_TYPE= 'm1.medium'  # 'c3.8xlarge'
    EC2_BEST_SLAVE_SPOT_PRICE=0.458900
    EC2_MASTER_INSTANCE_TYPE= 'm1.medium'  # 'm4.large'

    if not EC2_BEST_SLAVE_SPOT_PRICE:
        raise EmrSpotAirflowException('EmrSpotAirflowException failed recovering EC2_BEST_SLAVE_SPOT_PRICE')

    job_flow_overrides_runtime_method = {
        "AvailabilityZone": EC2_BEST_AVAILABILITY_ZONE,
        "Instances": {
            "InstanceGroups": [
                {
                     "Name": "Master nodes",
                     "Market": "ON_DEMAND",
                     "InstanceRole": "MASTER",
                     "InstanceType": EC2_MASTER_INSTANCE_TYPE,
                     "InstanceCount": 1
                },
                {
                     "Name": "Slave nodes",
                     "Market": "SPOT",
                     "InstanceRole": "CORE",
                     "BidPrice": str(EC2_MAX_CORE_BID_PRICE_ON_RUNNING),
                     "InstanceType": EC2_BEST_SLAVE_INSTANCE_TYPE,
                     "InstanceCount": 1
                }
            ]
        }
     }

    return job_flow_overrides_runtime_method


dag = DAG(
    'emr_job_flow_wikido_launch_biweekly_crawler2',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=26),
    schedule_interval='@once'
)

job_flow_creator = EmrCreateJobFlowOperatorWithRuntimeEvaluator(
    task_id='create_job_flow_spot',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    job_flow_overrides_runtime_method=job_flow_overrides_runtime_method,
    aws_conn_id='aws_default',
    emr_conn_id='emr_default',
    dag=dag
)

job_sensor = EmrJobFlowSensor(
    task_id='check_job_flow_spot',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow_spot', key='return_value') }}",
    aws_conn_id='aws_default',
    dag=dag
)

job_flow_creator.set_downstream(job_sensor)
