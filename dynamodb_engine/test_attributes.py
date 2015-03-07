""" Testing the Attributes """
import unittest

from .attributes import (
    StringAttribute,
    NumberAttribute)
from .models import Model
from . import types

DYNAMODB_HOST = 'localhost'
DYNAMODB_PORT = 8000


class User(Model):
    """ Basic user model """
    class Meta:
        table_name = 'users'
        dynamodb_local = {
            'host': DYNAMODB_HOST,
            'port': DYNAMODB_PORT
        }

    email = StringAttribute('email', hash_key=True)
    first_name = StringAttribute('firstName')
    last_name = StringAttribute('lastName')
    age = NumberAttribute('age')
    score = NumberAttribute('score')


class TestStringAttribute(unittest.TestCase):
    """ Test StringAttribute """
    def setUp(self):
        """ Set up method """
        self.user = User()
        self.user.email = 'sebastian.dahlgren@skymill.se'
        self.user.first_name = 'Sebastian'
        self.user.last_name = 'Dahlgren'
        self.user.age = 30
        self.user.score = 10.0
        self.user.create_table()
        self.user.save()

    def tearDown(self):
        """ Tear down method """
        self.user.delete_table()
