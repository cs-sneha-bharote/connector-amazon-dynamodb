""" Copyright start
  Copyright (C) 2008 - 2022 Fortinet Inc.
  All rights reserved.
  FORTINET CONFIDENTIAL & FORTINET PROPRIETARY SOURCE CODE
  Copyright end """

from connectors.core.connector import get_logger, ConnectorError
import boto3
from .constant import *

logger = get_logger('aws-dynamodb')


class DynamoDB(object):

    def __init__(self, config):
        self.config_type = config.get('config_type')
        self.aws_access_key_id = config.get('aws_access_key_id')
        self.aws_secret_access_key = config.get('aws_secret_access_key')
        self.region_name = config.get('region_name')
        self.aws_iam_role = config.get('aws_iam_role')

    def _get_dynamodb_client(self):
        try:
            if self.config_type == 'Assume Role':
                sts_client = boto3.client('sts', aws_access_key_id=self.aws_access_key_id,
                                          aws_secret_access_key=self.aws_secret_access_key)
                sts_response = sts_client.assume_role(RoleArn=self.aws_iam_role, RoleSessionName='DynamoDB')
                client = boto3.client(
                    'dynamodb',
                    aws_access_key_id=sts_response.get('Credentials').get('AccessKeyId'),
                    aws_secret_access_key=sts_response.get('Credentials').get('SecretAccessKey'),
                    aws_session_token=sts_response.get('Credentials').get('SessionToken'),
                    region_name=self.region_name
                )
            else:
                client = boto3.client(
                    'dynamodb',
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key,
                    region_name=self.region_name
                )
            return client
        except Exception as err:
            logger.error('{}'.format(str(err)))
            raise ConnectorError(str(err))

    def _convert_csv_str_to_list(self, list_param):
        try:
            if isinstance(list_param, str):
                return list_param.split(',')
            elif isinstance(list_param, list):
                return list_param
            else:
                raise ConnectorError("{} is not in a valid list or csv format".format(list_param))
        except Exception as err:
            logger.error('{}'.format(str(err)))
            raise ConnectorError(str(err))

    def _get_attribute_mapping(self, val, mapping_object):
        try:
            if val not in mapping_object.keys():
                return val
            else:
                return mapping_object.get(val)
        except Exception as err:
            logger.error('{}'.format(str(err)))
            raise ConnectorError(str(err))

    def _create_attribute_dict(self, params, dict_object, mapping_object=None):
        try:
            if mapping_object is not None:
                result = {k: self._get_attribute_mapping(params.get(v), mapping_object) for k, v in dict_object.items()}
            else:
                result = {k: params.get(v) for k, v in dict_object.items()}
            return result
        except Exception as err:
            logger.error('{}'.format(str(err)))
            raise ConnectorError(str(err))

    def _get_billing_mode_attribute(self, params):
        try:
            billing_mode = {}
            if params.get('billingMode') == 'Provisioned':
                billing_mode['BillingMode'] = self._get_attribute_mapping(params.get('billingMode'), BILL_MODE_MAPPING)
                billing_mode['ProvisionedThroughput'] = self._create_attribute_dict(params, PROVISIONED_THROUGHPUT)
            else:
                billing_mode['BillingMode'] = self._get_attribute_mapping(params.get('billingMode'), BILL_MODE_MAPPING)
            return billing_mode
        except Exception as err:
            logger.error('{}'.format(str(err)))
            raise ConnectorError(str(err))

    def _get_attribute_definition(self, params):
        try:
            attrib_definition = {}
            if params.get('sortKey'):
                attrib_definition['AttributeDefinitions'] = [(self._create_attribute_dict(params, item, DATA_TYPE_MAPPING)) for item in ATTRIBUTE_DEFINITIONS]
            else:
                attrib_definition['AttributeDefinitions'] = [self._create_attribute_dict(params, ATTRIBUTE_DEFINITIONS[0], DATA_TYPE_MAPPING)]
            return attrib_definition
        except Exception as err:
            logger.error('{}'.format(str(err)))
            raise ConnectorError(str(err))

    def _get_key_schema(self, params):
        try:
            key_schema = {}
            params.update(KEY_ROLE_MAPPING)
            if params.get('sortKey'):
                key_schema['KeySchema'] = [(self._create_attribute_dict(params, item)) for item in KEY_SCHEMA]
            else:
                key_schema['KeySchema'] = [self._create_attribute_dict(params, KEY_SCHEMA[0])]
            return key_schema
        except Exception as err:
            logger.error('{}'.format(str(err)))
            raise ConnectorError(str(err))

    def _get_db_stream_attribute(self, params):
        try:
            db_stream = {}
            if params.get('streamEnabled') == 'Enable':
                params.update({'streamEnabled': True})
                db_stream['StreamSpecification'] = self._create_attribute_dict(params, STREAM_SPECIFICATION, STREAM_VIEW_TYPE_MAPPING)
            else:
                db_stream['StreamSpecification'] = {'StreamEnabled': False}
            return db_stream
        except Exception as err:
            logger.error('{}'.format(str(err)))
            raise ConnectorError(str(err))

    def _get_projection_attribute(self, params):
        try:
            projection_attrib = {'ProjectionType': self._get_attribute_mapping(params.get('projection'), PROJECTION_MAPPING)}
            if params.get('projection') == 'Include':
                projection_attrib['NonKeyAttributes'] = self._convert_csv_str_to_list(params.get('nonKeyAttributes'))
            return projection_attrib
        except Exception as err:
            logger.error('{}'.format(str(err)))
            raise ConnectorError(str(err))

    def _get_global_secondary_index_attribute(self, params, action):
        try:
            if action == 'delete':
                Delete = {'IndexName': params.get('indexName')}
                return {'Delete': Delete}
            elif action == 'update':
                Update = {'IndexName': params.get('indexName')}
                Update['ProvisionedThroughput'] = self._create_attribute_dict(params, PROVISIONED_THROUGHPUT)
                return {'Update': Update}
            elif action == 'create':
                Create = {'IndexName': params.get('indexName')}
                Create.update(self._get_key_schema(params))
                Create['ProvisionedThroughput'] = self._create_attribute_dict(params, PROVISIONED_THROUGHPUT)
                Create['Projection'] = self._get_projection_attribute(params)
                return {'Create': Create}
        except Exception as err:
            logger.error('{}'.format(str(err)))
            raise ConnectorError(str(err))


    def _build_create_table_payload(self, params):
        try:
            payload = {'TableName': params.get('TableName')}
            payload.update(self._get_attribute_definition(params))
            payload.update(self._get_key_schema(params))
            payload.update(self._get_billing_mode_attribute(params))

            return payload
        except Exception as err:
            logger.error('{}'.format(str(err)))
            raise ConnectorError(str(err))

    def _build_update_table_payload(self, params):
        try:
            payload = {'TableName': params.get('TableName')}

            if params.get('updateOperation') == 'Modify Provisioned Throughput':
                payload.update(self._get_billing_mode_attribute(params))

            elif params.get('updateOperation') == 'Enable or Disable DynamoDB Streams':
                payload.update(self._get_db_stream_attribute(params))

            elif params.get('updateOperation') == 'Create a New Global Secondary Index':
                payload.update(self._get_attribute_definition(params))
                payload['GlobalSecondaryIndexUpdates'] = [self._get_global_secondary_index_attribute(params, action='create')]

            elif params.get('updateOperation') == 'Update a Global Secondary Index':
                payload['GlobalSecondaryIndexUpdates'] = [self._get_global_secondary_index_attribute(params, action='update')]

            elif params.get('updateOperation') == 'Remove a Global Secondary Index':
                payload['GlobalSecondaryIndexUpdates'] = [self._get_global_secondary_index_attribute(params, action='delete')]

            return payload
        except Exception as err:
            logger.error('{}'.format(str(err)))
            raise ConnectorError(str(err))

    def _build_add_item_payload(self, params):
        try:
            payload = {'TableName': params.get('TableName'), 'Item': {}}
            payload['Item'].update(
                {
                    params.get('partitionKeyName'):                         {
                        self._get_attribute_mapping(params.get('partitionKeyDataType'), DATA_TYPE_MAPPING): str(params.get('partitionKeyValue'))
                    }
                }
            )
            if params.get('sortKey'):
                payload['Item'].update(
                    {
                        params.get('sortKeyName'): {
                            self._get_attribute_mapping(params.get('sortKeyDataType'), DATA_TYPE_MAPPING): str(params.get('sortKeyValue'))
                        }
                    }
                )
            if params.get('additionalAttributes'):
                payload['Item'].update(params.get('additionalAttributes'))
            return payload
        except Exception as err:
            logger.error('{}'.format(str(err)))
            raise ConnectorError(str(err))


    def _build_delete_or_search_item_payload(self, params):
        try:
            payload = {'TableName': params.get('TableName'), 'Key': {}}
            payload['Key'].update({params.get('partitionKeyName'): {self._get_attribute_mapping(params.get('partitionKeyDataType'), DATA_TYPE_MAPPING): str(params.get('partitionKeyValue'))}})
            if params.get('sortKey'):
                payload['Key'].update({params.get('sortKeyName'): {self._get_attribute_mapping(params.get('sortKeyDataType'), DATA_TYPE_MAPPING): str(params.get('sortKeyValue'))}})
            if params.get('additionalAttributes'):
                payload['Key'].update(params.get('additionalAttributes'))
            return payload
        except Exception as err:
            logger.error('{}'.format(str(err)))
            raise ConnectorError(str(err))


    def _build_create_global_table_payload(self, params):
        try:
            payload = {'GlobalTableName': params.get('globalTableName'), 'ReplicationGroup': []}
            payload['ReplicationGroup'].append({'RegionName': params.get('regionName')})
            return payload
        except Exception as err:
            logger.error('{}'.format(str(err)))
            raise ConnectorError(str(err))
