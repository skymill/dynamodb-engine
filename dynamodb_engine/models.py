""" Model definitions """
from boto.dynamodb2.fields import HashKey, RangeKey
from boto.dynamodb2.items import Item
from boto.dynamodb2.table import Table
from boto.dynamodb2.exceptions import ValidationException
from boto.dynamodb2.exceptions import QueryError as BotoQueryError
from boto.exception import JSONResponseError
from six import with_metaclass

from .attributes import Attribute
from .connection import get_connection, DEFAULT_CONNECTION_NAME
from .exceptions import (
    ConnectionNotFoundError,
    MissingTableNameError,
    MissingHashKeyError,
    TableAlreadyExistsError,
    TableDeletionError,
    TableDoesNotExistError,
    TableUnknownError,
    QueryError,
    ValidationError)

# Meta data defaults
DEFAULT = {
    'table_name': None,
    'throughput': {
        'read': 1,
        'write': 1
    }
}


class ModelMetaDefault(object):
    """ Default Meta data """
    table_name = DEFAULT['table_name']
    throughput = DEFAULT['throughput']


class ModelMeta(type):
    """ Meta class for models

    Possible variables:
        - table_name
        - throughput
    """
    def __init__(cls, name, bases, attrs):
        if isinstance(attrs, dict):
            for attr_name, attr_obj in attrs.items():
                if attr_name == 'Meta':
                    if not hasattr(attr_obj, 'table_name'):
                        setattr(attr_obj, 'table_name', DEFAULT['table_name'])

                    if hasattr(attr_obj, 'throughput'):
                        throughput = DEFAULT['throughput']
                        if 'read' in getattr(attr_obj, 'throughput'):
                            throughput['read'] = getattr(
                                attr_obj, 'throughput')['read']
                        if 'write' in getattr(attr_obj, 'throughput'):
                            throughput['write'] = getattr(
                                attr_obj, 'throughput')['write']
                        setattr(attr_obj, 'throughput', throughput)
                    else:
                        setattr(attr_obj, 'throughput', DEFAULT['throughput'])

            # If no Meta class was defined, instanciate a default Meta class
            if 'Meta' not in attrs:
                setattr(cls, 'Meta', ModelMetaDefault)


class Model(with_metaclass(ModelMeta)):
    """ DynamoDBEngine Model """
    _connection = None
    _table = None
    _item = None

    def __init__(self, connection=DEFAULT_CONNECTION_NAME):
        """ Constructor for the model """
        # Connect to DynamoDB
        try:
            self._connection = get_connection(DEFAULT_CONNECTION_NAME)
        except ConnectionNotFoundError:
            raise

        self._get_table()

    def create_table(self, recreate=False):
        """ Create a DynamoDB table

        :type recreate: bool
        :param recreate: If the table exists, delete it and create a new
        """
        if not self.Meta.table_name:
            raise MissingTableNameError

        # Extract keys
        hash_key = self._get_hash_key()
        range_key = self._get_range_key()

        if not hash_key:
            raise MissingHashKeyError

        # Create schema
        schema = [
            HashKey(
                hash_key.get_name(),
                data_type=hash_key.get_type())
        ]
        if range_key:
            schema.append(
                RangeKey(
                    range_key.get_name(),
                    data_type=range_key.get_type()))

        try:
            self._table = Table.create(
                self.Meta.table_name,
                schema=schema,
                throughput={
                    'read': self.Meta.throughput['read'],
                    'write': self.Meta.throughput['write']
                },
                connection=self._connection)
        except JSONResponseError as error:
            if error.body['Message'] == 'Cannot create preexisting table':
                if recreate:
                    self.delete_table()
                    self.create_table()
                else:
                    raise TableAlreadyExistsError

    def delete_table(self):
        """ Delete the DynamoDB table """
        try:
            if self._table.delete():
                return
            else:
                raise TableDeletionError
        except JSONResponseError as error:
            non_existing_msg = 'Cannot do operations on a non-existent table'
            if error.body['Message'] == non_existing_msg:
                raise TableDoesNotExistError(error.body['Message'])
            else:
                raise TableDeletionError
        except AttributeError:
            raise TableDoesNotExistError

    def describe_table(self):
        """ Describe the DynamoDB table

        :returns: dict -- DynamoDB table configuration
        """
        try:
            return self._table.describe()
        except JSONResponseError as error:
            msgs = [
                'Cannot do operations on a non-existent table',
                'Requested resource not found']
            if error.body['Message'].startswith(msgs[1]):
                raise TableDoesNotExistError(error.body['Message'])
            elif error.body['Message'] == msgs[0]:
                raise TableDoesNotExistError(error.body['Message'])
            else:
                raise TableUnknownError(error.body['Message'])
        except AttributeError:
            raise TableDoesNotExistError

    def save(self, overwrite=False):
        """ Save the item

        :type overwrite: bool
        :param overwrite: Set to True if we should overwrite the item in the DB
        """
        # Create new Item object
        if not self._item:
            self._item = Item(
                self._table,
                data={
                    attr.get_name(): attr.get_value()
                    for attr in self._get_attributes()
                })

        # Update existing object
        else:
            for attr in self._get_attributes():
                self._item[attr.get_name()] = attr.get_value()

        try:
            self._item.save(overwrite=overwrite)
        except ValidationException as error:
            raise ValidationError(error.body['Message'])

    def query(self, *args, **kwargs):
        """ Query the database

        :type *args: *args
        :param *args: Arguments
        :type **kwargs: **kwargs
        :param **kwargs: Key word arguments
        :returns: boto.dynamodb2.results - Result set
        """
        try:
            return self._table.query_2(*args, **kwargs)
        except BotoQueryError as error:
            raise QueryError(error)

    def query_count(self, *args, **kwargs):
        """ Return the number of matches for a query

        :type *args: *args
        :param *args: Arguments
        :type **kwargs: **kwargs
        :param **kwargs: Key word arguments
        :returns: int -- Number of matches
        """
        try:
            return self._table.query_count(*args, **kwargs)
        except BotoQueryError as error:
            raise QueryError(error)

    @classmethod
    def _get_attributes(cls):
        """ Get attributes defined in this class

        Excludes classes, functions and variables starting with _

        :type cls: Class instance
        :param cls: Instance object for this current class
        :returns: list -- List of attributes
        """
        attrs = []
        for attr in vars(cls):
            if attr.startswith('_'):
                continue

            attr_obj = getattr(cls, attr)
            attr_cls = getattr(attr_obj, '__class__', None)
            if not attr_cls:
                continue

            if issubclass(attr_cls, (Attribute, )):
                attrs.append(attr_obj)

        return attrs

    def _get_hash_key(self):
        """ Return the hash key attribute """
        for attribute in self._get_attributes():
            if attribute.is_hash_key():
                return attribute

    def _get_range_key(self):
        """ Return the range key attribute """
        for attribute in self._get_attributes():
            if attribute.is_range_key():
                return attribute

    def _get_table(self):
        """ Get the table and populate self._table """
        for table_name in self._connection.list_tables():
            if table_name == self.Meta.table_name:
                self._table = Table(
                    self.Meta.table_name,
                    connection=self._connection)
