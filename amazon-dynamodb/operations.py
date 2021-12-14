""" Copyright start
  Copyright (C) 2008 - 2021 Fortinet Inc.
  All rights reserved.
  FORTINET CONFIDENTIAL & FORTINET PROPRIETARY SOURCE CODE
  Copyright end """

from connectors.core.connector import get_logger, ConnectorError
from .utils import DynamoDB
import functools
import operator

logger = get_logger('amazon-dynamodb')


def create_table(config, params):
    try:
        dynamoDB = DynamoDB(config)
        dynamoDB_client = dynamoDB._get_dynamodb_client()
        payload = dynamoDB._build_create_table_payload(params)
        response = dynamoDB_client.create_table(**payload)
        response.pop('ResponseMetadata')
        return response
    except Exception as err:
        logger.error('{}'.format(str(err)))
        raise ConnectorError(str(err))


def delete_table(config, params):
    try:
        dynamoDB = DynamoDB(config)
        dynamoDB_client = dynamoDB._get_dynamodb_client()
        response = dynamoDB_client.delete_table(**params)
        response.pop('ResponseMetadata')
        return response
    except Exception as err:
        logger.error('{}'.format(str(err)))
        raise ConnectorError(str(err))


def update_table(config, params):
    try:
        dynamoDB = DynamoDB(config)
        dynamoDB_client = dynamoDB._get_dynamodb_client()
        payload = dynamoDB._build_update_table_payload(params)
        response = dynamoDB_client.update_table(**payload)
        response.pop('ResponseMetadata')
        return response
    except Exception as err:
        logger.error('{}'.format(str(err)))
        raise ConnectorError(str(err))


def get_table_list(config, params):
    try:
        dynamoDB = DynamoDB(config)
        dynamoDB_client = dynamoDB._get_dynamodb_client()
        paginator = dynamoDB_client.get_paginator('list_tables').paginate()
        table_list = [page['TableNames'] for page in paginator]
        response = functools.reduce(operator.iconcat, table_list, [])
        return response
    except Exception as err:
        logger.error('{}'.format(str(err)))
        raise ConnectorError(str(err))


def get_table_details(config, params):
    try:
        dynamoDB = DynamoDB(config)
        dynamoDB_client = dynamoDB._get_dynamodb_client()
        response = dynamoDB_client.describe_table(**params)
        response.pop('ResponseMetadata')
        return response
    except Exception as err:
        logger.error('{}'.format(str(err)))
        raise ConnectorError(str(err))


def add_item(config, params):
    try:
        dynamoDB = DynamoDB(config)
        dynamoDB_client = dynamoDB._get_dynamodb_client()
        payload = dynamoDB._build_add_item_payload(params)
        response = dynamoDB_client.put_item(**payload)
        response.pop('ResponseMetadata')
        return response
    except Exception as err:
        logger.error('{}'.format(str(err)))
        raise ConnectorError(str(err))


def delete_item(config, params):
    try:
        dynamoDB = DynamoDB(config)
        dynamoDB_client = dynamoDB._get_dynamodb_client()
        payload = dynamoDB._build_delete_or_search_item_payload(params)
        response = dynamoDB_client.delete_item(**payload)
        response.pop('ResponseMetadata')
        return response
    except Exception as err:
        logger.error('{}'.format(str(err)))
        raise ConnectorError(str(err))


def search_item(config, params):
    try:
        dynamoDB = DynamoDB(config)
        dynamoDB_client = dynamoDB._get_dynamodb_client()
        payload = dynamoDB._build_delete_or_search_item_payload(params)
        response = dynamoDB_client.get_item(**payload)
        response.pop('ResponseMetadata')
        return response
    except Exception as err:
        logger.error('{}'.format(str(err)))
        raise ConnectorError(str(err))


def create_global_table(config, params):
    try:
        dynamoDB = DynamoDB(config)
        dynamoDB_client = dynamoDB._get_dynamodb_client()
        payload = dynamoDB._build_create_global_table_payload(params)
        response = dynamoDB_client.create_global_table(**payload)
        response.pop('ResponseMetadata')
        return response
    except Exception as err:
        logger.error('{}'.format(str(err)))
        raise ConnectorError(str(err))


def describe_global_table(config, params):
    try:
        dynamoDB = DynamoDB(config)
        dynamoDB_client = dynamoDB._get_dynamodb_client()
        response = dynamoDB_client.describe_global_table(GlobalTableName=params.get('globalTableName'))
        response.pop('ResponseMetadata')
        return response
    except Exception as err:
        logger.error('{}'.format(str(err)))
        raise ConnectorError(str(err))


def get_global_table_list(config, params):
    try:
        dynamoDB = DynamoDB(config)
        dynamoDB_client = dynamoDB._get_dynamodb_client()
        response = dynamoDB_client.list_global_tables()
        response.pop('ResponseMetadata')
        return response
    except Exception as err:
        logger.error('{}'.format(str(err)))
        raise ConnectorError(str(err))


def create_backup(config, params):
    try:
        dynamoDB = DynamoDB(config)
        dynamoDB_client = dynamoDB._get_dynamodb_client()
        response = dynamoDB_client.create_backup(**params)
        response.pop('ResponseMetadata')
        return response
    except Exception as err:
        logger.error('{}'.format(str(err)))
        raise ConnectorError(str(err))


def describe_table_backup(config, params):
    try:
        dynamoDB = DynamoDB(config)
        dynamoDB_client = dynamoDB._get_dynamodb_client()
        response = dynamoDB_client.describe_backup(**params)
        response.pop('ResponseMetadata')
        return response
    except Exception as err:
        logger.error('{}'.format(str(err)))
        raise ConnectorError(str(err))


def delete_table_backup(config, params):
    try:
        dynamoDB = DynamoDB(config)
        dynamoDB_client = dynamoDB._get_dynamodb_client()
        response = dynamoDB_client.delete_backup(**params)
        response.pop('ResponseMetadata')
        return response
    except Exception as err:
        logger.error('{}'.format(str(err)))
        raise ConnectorError(str(err))


def health_check(config):
    try:
        dynamoDB = DynamoDB(config)
        dynamoDB_client = dynamoDB._get_dynamodb_client()
        response = dynamoDB_client.list_tables(Limit=1)
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return True
    except Exception as err:
        logger.error('{}'.format(str(err)))
        raise ConnectorError(str(err))


operations = {
    'create_table': create_table,
    'delete_table': delete_table,
    'update_table': update_table,
    'get_table_list': get_table_list,
    'get_table_details': get_table_details,
    'add_item': add_item,
    'delete_item': delete_item,
    'search_item': search_item,
    'create_global_table': create_global_table,
    'describe_global_table': describe_global_table,
    'get_global_table_list': get_global_table_list,
    'create_backup': create_backup,
    'describe_table_backup': describe_table_backup,
    'delete_table_backup': delete_table_backup
}
