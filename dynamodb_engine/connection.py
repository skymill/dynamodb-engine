"""
Connection handling
"""
from boto.dynamodb2 import connect_to_region
from boto.dynamodb2.layer1 import DynamoDBConnection

from .exceptions import (
    DynamoDBConnectionError,
    DynamoDBLocalConnectionError)

CONNECTION = None


def connect(
        region='us-east-1',
        dynamodb_local_host=None,
        dynamodb_local_port=8000):
    """ Create a DynamoDB connection

    :type region: str
    :param region: AWS region
    :type dynamodb_local_host: str
    :param dynamodb_local_host:
        Hostname for DynamoDB Local. If this is not None a connection to
        DynamoDB Local will be done rather than a connection to AWS
    :type dynamodb_local_port: int
    :param dynamodb_local_port: Port number for DynamoDB Local
    :returns: boto.dynamodb2.layer1.DynamoDBConnection -- Connection
    """
    global CONNECTION
    if CONNECTION:
        return CONNECTION

    if not dynamodb_local_host:
        CONNECTION = connect_aws(region)
    else:
        CONNECTION = connect_local(dynamodb_local_host, dynamodb_local_port)

    return CONNECTION


def connect_aws(region):
    """
    Create a connection to DynamoDB

    :type region: str
    :param region: AWS region to connect to
    :returns: boto.dynamodb2.layer1.DynamoDBConnection
    """
    try:
        return connect_to_region(region)
    except Exception as error:
        raise DynamoDBConnectionError(error)


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
    except Exception as error:
        raise DynamoDBLocalConnectionError(error)
