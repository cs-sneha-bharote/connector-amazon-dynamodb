""" Copyright start
  Copyright (C) 2008 - 2021 Fortinet Inc.
  All rights reserved.
  FORTINET CONFIDENTIAL & FORTINET PROPRIETARY SOURCE CODE
  Copyright end """

from connectors.core.connector import get_logger, ConnectorError, Connector
from .operations import operations, health_check

logger = get_logger('amazon-dynamodb')


class AmazonDynamoDB(Connector):
    try:
        def execute(self, config, operation, params, **kwargs):
            action = operations.get(operation)
            return action(config, params)
    except Exception as err:
        logger.exception('An exception occurred {}'.format(str(err)))
        raise ConnectorError(str(err))

    def check_health(self, config):
        return health_check(config)
