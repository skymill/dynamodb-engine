"""
Model definitions
"""
from boto.dynamodb2.fields import HashKey, RangeKey
from boto.dynamodb2.items import Item
from boto.dynamodb2.table import Table
from boto.dynamodb2.exceptions import QueryError as BotoQueryError
from boto.exception import JSONResponseError
from six import with_metaclass

from .attributes import Attribute
from .connection import connect
from .exceptions import (
    MissingTableNameError,
    MissingHashKeyError,
    TableAlreadyExistsError,
    TableDeletionError,
    QueryError)

# Meta data defaults
DEFAULT = {
    'dynamodb_local': None,
    'region': 'us-east-1',
    'table_name': None,
    'throughput': {
        'read': 1,
        'write': 1
    }
}


class ModelMetaDefault(object):
    """
    Default Meta data
    """
    dynamodb_local = DEFAULT['dynamodb_local']
    table_name = DEFAULT['table_name']
    region = DEFAULT['region']
    throughput = DEFAULT['throughput']


class ModelMeta(type):
    """
    Meta class for models

    Possible variables:
        - dynamodb_local
        - region
        - table_name
        - throughput
    """
    def __init__(cls, name, bases, attrs):
        if isinstance(attrs, dict):
            for attr_name, attr_obj in attrs.items():
                if attr_name == 'Meta':
                    if not hasattr(attr_obj, 'table_name'):
                        setattr(attr_obj, 'table_name', DEFAULT['table_name'])

                    if not hasattr(attr_obj, 'region'):
                        setattr(attr_obj, 'region', DEFAULT['region'])

                    if not hasattr(attr_obj, 'dynamodb_local'):
                        setattr(
                            attr_obj,
                            'dynamodb_local',
                            DEFAULT['dynamodb_local'])

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
    """
    DynamoDBEngine Model
    """
    _connection = None
    _table = None
    _item = None

    def __init__(self):
        """
        Constructor for the model
        """
        # Connect to DynamoDB
        self._get_attributes()
        self._connect()
        self._get_table()

    def create_table(self, recreate=False):
        """
        Create a DynamoDB table

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
        schema = [HashKey(hash_key.get_name())]
        if range_key:
            schema.append(RangeKey(range_key.get_name()))

        try:
            table = Table.create(
                self.Meta.table_name,
                schema=schema,
                throughput={
                    'read': self.Meta.throughput['read'],
                    'write': self.Meta.throughput['write']
                },
                connection=self._connection)

            self._table = table
        except JSONResponseError as error:
            if error.body['Message'] == 'Cannot create preexisting table':
                if recreate:
                    self.delete_table()
                    self.create_table()
                else:
                    raise TableAlreadyExistsError

    def delete_table(self):
        """
        Delete the DynamoDB table
        """
        if self._table.delete():
            return
        else:
            raise TableDeletionError

    def describe_table(self):
        """
        Describe the DynamoDB table

        :returns: dict -- DynamoDB table configuration
        """
        return self._table.describe()

    def save(self, overwrite=False):
        """
        Save the item

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

        self._item.save(overwrite=overwrite)

    def query(self, *args, **kwargs):
        """
        Query the database

        See http://boto.readthedocs.org/en/latest/ref/dynamodb2.html#boto.dynamodb2.table.Table.query_2 for usage details

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
        """
        Return the number of matches for a query

        See http://boto.readthedocs.org/en/latest/ref/dynamodb2.html#boto.dynamodb2.table.Table.query_count for usage details

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

    def _connect(self):
        """
        Connect to DynamoDB
        """
        try:
            self._connection = connect(self.Meta)
        except Exception:
            raise

    @classmethod
    def _get_attributes(cls):
        """
        Get attributes defined in this class

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
        """
        Return the hash key attribute
        """
        for attribute in self._get_attributes():
            if attribute.is_hash_key():
                return attribute

    def _get_range_key(self):
        """
        Return the range key attribute
        """
        for attribute in self._get_attributes():
            if attribute.is_range_key():
                return attribute

    def _get_table(self):
        """
        Get the table and populate self._table
        """
        self._table = Table(
            self.Meta.table_name,
            connection=self._connection)
