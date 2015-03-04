"""
Attributes implementations
"""

from dynamodb_engine import types


class Attribute(object):
    """
    Base attribute object
    """
    attr_name = None
    attr_value = None
    attr_type = types.STRING
    hash_key = False
    range_key = False
    null = False

    def __init__(
            self,
            name,
            attr_type=types.STRING,
            hash_key=False,
            range_key=False,
            null=False):
        """
        Constructor for the attribute

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
        self.hash_key = hash_key
        self.range_key = range_key
        self.null = null


class NumberAttribute(Attribute):
    """
    Number attribute class
    """
    def __init__(
            self,
            name,
            hash_key=False,
            range_key=False,
            null=False):
        """
        Constructor for the attribute

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


class StringAttribute(Attribute):
    """
    String attribute class
    """
    def __init__(
            self,
            name,
            hash_key=False,
            range_key=False,
            null=False):
        """
        Constructor for the attribute

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
