"""
Exceptions
"""


class DynamoDBEngineException(Exception):
    """
    DynamoDB Engine base error
    """
    message = None

    def __init__(self, message=None):
        """ Constructor """
        self.message = message or self.message

        super(DynamoDBEngineException, self).__init__(self.message)


class MissingDynamoDBLocalHostError(DynamoDBEngineException):
    """
    Missing host name for connecting to DynamoDB Local
    """
    message = 'Missing host for DynamoDB Local connection'


class MissingDynamoDBLocalPortError(DynamoDBEngineException):
    """
    Missing port name for connecting to DynamoDB Local
    """
    message = 'Missing port for DynamoDB Local connection'


class MissingHashKeyError(DynamoDBEngineException):
    """
    Missing hash key in model
    """
    message = 'Missing hash key in model'


class MissingTableNameError(DynamoDBEngineException):
    """
    Missing table name in Meta configuration for a Model
    """
    message = 'Missing table_name in Meta class'
