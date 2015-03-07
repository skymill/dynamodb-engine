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


class AttributeException(DynamoDBEngineException):
    """
    Attribute exception
    """
    message = 'Attribute exception'


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


class TableAlreadyExistsError(DynamoDBEngineException):
    """
    The table we're trying to create already exists
    """
    message = 'Table already exists'


class TableDeletionError(DynamoDBEngineException):
    """
    An error occurred deleting the table
    """
    message = 'An unknown error occurred deleting the table'


class TableDoesNotExistError(DynamoDBEngineException):
    """
    The table does not exist
    """
    message = 'The table does not exist'


class TableUnknownError(DynamoDBEngineException):
    """
    An unknown error occurred in the table communication
    """
    message = 'An unknown error occurred communicating with the table'


class QueryError(DynamoDBEngineException):
    """
    Query error
    """
    pass
