""" Connection handling """
from boto.dynamodb2 import connect_to_region
from boto.dynamodb2.layer1 import DynamoDBConnection

from .exceptions import (
    ConnectionNotFoundError,
    DynamoDBConnectionError,
    DynamoDBLocalConnectionError)

_CONNECTIONS = {}
DEFAULT_CONNECTION_NAME = 'default'


def connect(name=DEFAULT_CONNECTION_NAME, **kwargs):
    """ Get a connection

    :type name: str
    :param name: Connection name
    :type reconnect: bool
    :param reconnect: Reconnect if the connection already exists
    :returns: DynamoDBConnection or None -- None if not connected
    """
    global _CONNECTIONS

    if name not in _CONNECTIONS.keys():
        return register_connection(name, **kwargs)
    return get_connection(name)


def deregister_connection(name=DEFAULT_CONNECTION_NAME):
    """ Deregister connection

    :type name: str
    :param name: Connection to deregister
    :returns: None
    """
    global _CONNECTIONS

    if name in _CONNECTIONS.keys():
        del _CONNECTIONS[name]

    return None


def get_connection(name=DEFAULT_CONNECTION_NAME):
    """ Get connection by name

    :type name: str
    :param name: Connection name
    :returns: DynamoDBConnection or None -- Returns the connection object
    """
    global _CONNECTIONS
    try:
        return _CONNECTIONS[name]
    except KeyError:
        raise ConnectionNotFoundError(
            'Connection "{}" not found'.format(name))


def list_connections():
    """ List existing connections

    :returns: list -- List of connection names
    """
    global _CONNECTIONS

    return _CONNECTIONS.keys()


def register_connection(
        name=DEFAULT_CONNECTION_NAME,
        region='us-east-1',
        dynamodb_local_host=None,
        dynamodb_local_port=8000):
    """ Create a DynamoDB connection

    :type name: str
    :param name: Connection name (used if you need many connections)
    :type region: str
    :param region: AWS region
    :type dynamodb_local_host: str
    :param dynamodb_local_host:
        Hostname for DynamoDB Local. If this is not None a connection to
        DynamoDB Local will be done rather than a connection to AWS
    :type dynamodb_local_port: int
    :param dynamodb_local_port: Port number for DynamoDB Local
    """
    if dynamodb_local_host:
        connection = _connect_local(dynamodb_local_host, dynamodb_local_port)
    else:
        connection = _connect_aws(region)

    _CONNECTIONS[name] = connection

    return connection


def _connect_aws(region):
    """ Create a connection to DynamoDB

    :type region: str
    :param region: AWS region to connect to
    :returns: DynamoDBConnection -- DynamoDB connection object
    """
    try:
        return connect_to_region(region)
    except Exception as error:
        raise DynamoDBConnectionError(error)


def _connect_local(host, port):
    """ Connect to DynamoDB Local

    :type host: str
    :param host: Hostname
    :type port: int
    :param port: Port number
    :returns: DynamoDBConnection -- DynamoDB connection object
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
