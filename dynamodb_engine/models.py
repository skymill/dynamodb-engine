"""
Model definitions
"""
import inspect

from six import with_metaclass

from dynamodbengine.connection import connect

# Meta data defaults
DEFAULT = {
    'region': 'us-east-1',
    'table': None,
    'host': None
}


class ModelMetaDefault(object):
    """
    Default Meta data
    """
    table = DEFAULT['table']
    region = DEFAULT['region']
    host = DEFAULT['host']


class ModelMeta(type):
    """
    Meta class for models

    Possible variables:
        - table
        - region
        - host
    """
    def __init__(cls, name, bases, attrs):
        if isinstance(attrs, dict):
            for attr_name, attr_obj in attrs.items():
                if attr_name == 'Meta':
                    if not hasattr(attr_obj, 'table'):
                        setattr(attr_obj, 'table', DEFAULT['table'])
                    if not hasattr(attr_obj, 'region'):
                        setattr(attr_obj, 'region', DEFAULT['region'])
                    if not hasattr(attr_obj, 'host'):
                        setattr(attr_obj, 'host', DEFAULT['host'])

            # If no Meta class was defined, instanciate a default Meta class
            if 'Meta' not in attrs:
                setattr(cls, 'Meta', ModelMetaDefault)


class Model(with_metaclass(ModelMeta)):
    """
    DynamoDBEngine Model
    """
    _connection = None

    def __init__(self):
        """
        Constructor for the model
        """
        # Connect to DynamoDB
        self._connect()

    def _connect(self):
        """
        Connect to DynamoDB
        """
        try:
            self._connection = connect(self.Meta.region)
        except Exception:
            raise

    @classmethod
    def _get_attrs(cls):
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
            if inspect.isclass(getattr(cls, attr)):
                continue
            if inspect.isfunction(getattr(cls, attr)):
                continue

            attrs.append(attr)

        return attrs
