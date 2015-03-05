"""
Connection handling
"""
from boto.dynamodb2 import connect_to_region
from boto.dynamodb2.layer1 import DynamoDBConnection

from .exceptions import (
    MissingDynamoDBLocalHostError,
    MissingDynamoDBLocalPortError)


def connect(meta):
    """
    Connect to AWS or DynamoDB Local

    :type meta: dynamodb_engine.models.ModelMeta
    :param meta: Model meta data object
    :returns: boto.dynamodb2.layer1.DynamoDBConnection
    """
    if meta.dynamodb_local:
        if not meta.dynamodb_local['host']:
            raise MissingDynamoDBLocalHostError
        if not meta.dynamodb_local['port']:
            raise MissingDynamoDBLocalPortError
        return connect_local(
            meta.dynamodb_local['host'],
            meta.dynamodb_local['port'])

    return connect_aws(meta.region)


def connect_aws(region):
    """
    Create a connection to DynamoDB

    :type region: str
    :param region: AWS region to connect to
    :returns: boto.dynamodb2.layer1.DynamoDBConnection
    """
    try:
        return connect_to_region(region)
    except Exception:
        raise


def connect_local(host, port):
    """
    Connect to DynamoDB Local

    :type host: str
    :param host: Hostname
    :type port: int
    :param port: Port number
    :returns: boto.dynamodb2.layer1.DynamoDBConnection
    """
    try:
        return DynamoDBConnection(
            host=host,
            port=port,
            aws_access_key_id='foo',
            aws_secret_access_key='bar',
            is_secure=False)
    except Exception:
        raise
