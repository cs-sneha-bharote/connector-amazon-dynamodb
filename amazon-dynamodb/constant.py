""" Copyright start
  Copyright (C) 2008 - 2021 Fortinet Inc.
  All rights reserved.
  FORTINET CONFIDENTIAL & FORTINET PROPRIETARY SOURCE CODE
  Copyright end """

ATTRIBUTE_DEFINITIONS = (
    {
        'AttributeName': 'partitionKeyName',
        'AttributeType': 'partitionKeyDataType'
    },
    {
        'AttributeName': 'sortKeyName',
        'AttributeType': 'sortKeyDataType'
    }
)

KEY_SCHEMA = (
    {
        'AttributeName': 'partitionKeyName',
        'KeyType': 'partitionKeyRole'
    },
    {
        'AttributeName': 'sortKeyName',
        'KeyType': 'sortKeyRole'
    }
)

KEY_ROLE_MAPPING = {
    'partitionKeyRole': 'HASH',
    'sortKeyRole': 'RANGE'
}

DATA_TYPE_MAPPING = {
    'String': 'S',
    'Binary': 'B',
    'Number': 'N'
}

BILL_MODE_MAPPING = {
    'Provisioned': 'PROVISIONED',
    'On Demand': 'PAY_PER_REQUEST'
}

PROVISIONED_THROUGHPUT = {
    'ReadCapacityUnits': 'readCapacityUnits',
    'WriteCapacityUnits': 'writeCapacityUnits'
}

STREAM_SPECIFICATION = {
    'StreamEnabled': 'streamEnabled',
    'StreamViewType': 'streamViewType'
}

STREAM_VIEW_TYPE_MAPPING = {
    'Key attributes only': 'KEYS_ONLY',
    'New image': 'NEW_IMAGE',
    'Old image': 'OLD_IMAGE',
    'New and old images': 'NEW_AND_OLD_IMAGES'
}

PROJECTION_MAPPING = {
    'All': 'ALL',
    'Only Keys': 'KEYS_ONLY',
    'Include': 'INCLUDE'
}