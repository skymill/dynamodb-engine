""" Attributes implementations """

from . import types
from .exceptions import AttributeException
from .limits import NUMBER_ATTR_MAX


class Attribute(object):
    """ Base attribute object """
    def __init__(
            self,
            name,
            attr_type=types.STRING,
            hash_key=False,
            range_key=False,
            null=False):
        """ Constructor for the attribute

        :type name: str
        :param name: Name of the attribute
        :type attr_type: dynamodb_engine.types
        :param attr_type: Attribute type
        :type hash_key: bool
        :param hash_key: True if the attribute should be table hash key
        :type range_key: bool
        :param range_key: True if the attribute should be table range key
        :type null: bool
        :param null: Is the value allowed to be null?
        """
        self.attr_name = name
        self.attr_type = attr_type
        self.attr_value = None
        self.hash_key = hash_key
        self.range_key = range_key
        self.null = null

    def get_name(self):
        """ Get the attribute name """
        return self.attr_name

    def get_type(self):
        """ Get the attribute type """
        return self.attr_type

    def get_value(self):
        """ Get the valur if the attribute """
        return self.attr_value

    def is_hash_key(self):
        """ Check if the attribute is the hash key

        :returns: bool - True if it is the hash key
        """
        return self.hash_key

    def is_range_key(self):
        """ Check if the attribute is the range key

        :returns: bool - True if it is the range key
        """
        return self.range_key

    def __set__(self, obj, value):
        """ Setter """
        self.attr_value = value


class NumberAttribute(Attribute):
    """ Number attribute class """
    def __init__(
            self,
            name,
            hash_key=False,
            range_key=False,
            null=False):
        """ Constructor for the attribute

        :type name: str
        :param name: Name of the attribute
        :type hash_key: bool
        :param hash_key: True if the attribute should be table hash key
        :type range_key: bool
        :param range_key: True if the attribute should be table range key
        :type null: bool
        :param null: Is the value allowed to be null?
        """
        super(NumberAttribute, self).__init__(
            name,
            attr_type=types.NUMBER,
            hash_key=hash_key,
            range_key=range_key,
            null=null)

    def __set__(self, obj, value):
        """ Setter """
        if not isinstance(value, int) and not isinstance(value, float):
            raise AttributeException(
                '{} is not an integer nor float'.format(value))
        if len(str(value).replace('.', '').replace('-', '')) > 38:
            raise AttributeException(
                'Number "{}" is more than {} digits long'.format(
                    value,
                    NUMBER_ATTR_MAX))

        self.attr_value = value


class StringAttribute(Attribute):
    """ String attribute class """
    def __init__(
            self,
            name,
            hash_key=False,
            range_key=False,
            null=False):
        """ Constructor for the attribute

        :type name: str
        :param name: Name of the attribute
        :type hash_key: bool
        :param hash_key: True if the attribute should be table hash key
        :type range_key: bool
        :param range_key: True if the attribute should be table range key
        :type null: bool
        :param null: Is the value allowed to be null?
        """
        super(StringAttribute, self).__init__(
            name,
            attr_type=types.STRING,
            hash_key=hash_key,
            range_key=range_key,
            null=null)
