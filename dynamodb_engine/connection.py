"""
Connection handling
"""
from boto.dynamodb2 import connect_to_region


def connect(region):
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
