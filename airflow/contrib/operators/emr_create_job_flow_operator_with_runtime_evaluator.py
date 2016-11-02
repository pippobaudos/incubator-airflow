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

import logging

from airflow.contrib.hooks.emr_hook import EmrHook
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.models import BaseOperator
from airflow.utils import apply_defaults
from airflow.exceptions import AirflowException


class EmrSpotAirflowException(Exception):
    pass


class EmrCreateJobFlowOperatorWithRuntimeEvaluator(BaseOperator):
    """
    Creates an EMR JobFlow, reading the config from the EMR connection.
    A dictionary of JobFlow overrides can be passed that override the config from the connection.
    An additional method executed at runtime allow an additional override to the config from the connection.

    :param aws_conn_id: aws connection to uses
    :type aws_conn_id: str
    :param emr_conn_id: emr connection to use
    :type emr_conn_id: str
    :param job_flow_overrides: boto3 style arguments to override emr_connection extra
    :param runtime_job_flow_overrides: custom method evaluated at runtime to override emr_connection extra
    :                           Use to evaluate spot prices instantely before a task is launched
    :type steps: dict
    """
    template_fields = []
    template_ext = ()
    ui_color = '#f9c915'

    @apply_defaults
    def __init__(
            self,
            aws_conn_id='s3_default',
            emr_conn_id='emr_default',
            job_flow_overrides=None,
            job_flow_overrides_runtime_method=None,
            *args, **kwargs):
        super(EmrCreateJobFlowOperatorWithRuntimeEvaluator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.emr_conn_id = emr_conn_id
        if job_flow_overrides is None:
            job_flow_overrides = {}
        self.job_flow_overrides = job_flow_overrides

        # Evaluate if exist custom method to be evaluated at runtime...
        if job_flow_overrides_runtime_method is None:
            return

        logging.info('Specified runtime_method to define some job flow options.')
        job_flow_overrides_runtime = job_flow_overrides_runtime_method()
        if not job_flow_overrides_runtime:
            logging.info('No additional settings extracted by the runtime_method.')
            return

        logging.info('The following settings extracted by the runtime_method: %s', job_flow_overrides_runtime)
        job_flow_overrides = dict(job_flow_overrides.items() + job_flow_overrides_runtime.items())
        logging.info('The new job_flow_overrides defined is: %s', job_flow_overrides)
        self.job_flow_overrides = job_flow_overrides

    def execute(self, context):
        logging.info('Creating EmrHook')
        emr = EmrHook(aws_conn_id=self.aws_conn_id, emr_conn_id=self.emr_conn_id)

        logging.info('The new job_flow_overrides defined is: %s', self.job_flow_overrides)
        response = emr.create_job_flow(self.job_flow_overrides)

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise AirflowException('JobFlow creation failed: %s' % response)
        else:
            logging.info('JobFlow with id %s created', response['JobFlowId'])
            return response['JobFlowId']


