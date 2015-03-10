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
    home = StringAttribute('home', range_key=True)
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
        self.user.home = 'sweden'
        self.user.first_name = 'Sebastian'
        self.user.last_name = 'Dahlgren'
        self.user.age = 30
        self.user.score = 10.0
        self.user.create_table()
        self.user.save()

    def test_instance(self):
        """ Test instance type """
        self.assertIsInstance(self.user.first_name, StringAttribute)

    def test_name(self):
        """ Test the attribute name """
        self.assertEqual(self.user.first_name.get_name(), 'firstName')

    def test_is_hash_key(self):
        """ Test if the attribute is hash key """
        self.assertTrue(self.user.email.is_hash_key())

    def test_is_not_hash_key(self):
        """ Test if the attribute is hash key """
        self.assertFalse(self.user.first_name.is_hash_key())

    def test_is_range_key(self):
        """ Test if the attribute is range key """
        self.assertTrue(self.user.home.is_range_key())

    def test_is_not_range_key(self):
        """ Test if the attribute is range key """
        self.assertFalse(self.user.email.is_range_key())

    def test_type(self):
        """ Test type """
        self.assertEqual(self.user.first_name.get_type(), types.STRING)

    def test_value(self):
        """ Test value """
        self.assertEqual(self.user.first_name.get_value(), 'Sebastian')

    def tearDown(self):
        """ Tear down method """
        self.user.delete_table()
