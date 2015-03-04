"""
Model definitions
"""
from boto.dynamodb2.fields import HashKey, RangeKey
from boto.dynamodb2.table import Table
from six import with_metaclass

from dynamodb_engine.attributes import Attribute
from dynamodb_engine.connection import connect
from dynamodb_engine.exceptions import (
    MissingTableNameError,
    MissingHashKeyError)

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

    def __init__(self):
        """
        Constructor for the model
        """
        # Connect to DynamoDB
        self._connect()

    def create_table(self):
        """
        Create a DynamoDB table
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

        table = Table.create(
            self.Meta.table_name,
            schema=schema,
            throughput={
                'read': self.Meta.throughput['read'],
                'write': self.Meta.throughput['write']
            },
            connection=self._connection)

        self._table = table

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

            attr_cls = getattr(getattr(cls, attr), "__class__", None)
            if not attr_cls:
                continue

            if issubclass(attr_cls, (Attribute, )):
                attrs.append(getattr(cls, attr))

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
