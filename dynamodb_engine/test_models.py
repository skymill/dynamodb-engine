"""
Testing the Model
"""
import unittest

from .attributes import StringAttribute
from .exceptions import MissingHashKeyError
from .models import Model

DYNAMODB_HOST = 'localhost'
DYNAMODB_PORT = 8000


class User(Model):
    """ Basic User model used for testing purposes """
    class Meta:
        table_name = 'users'
        dynamodb_local = {
            'host': DYNAMODB_HOST,
            'port': DYNAMODB_PORT
        }

    email = StringAttribute('email', hash_key=True)
    first_name = StringAttribute('firstName')
    last_name = StringAttribute('lastName')


class TestModel(unittest.TestCase):
    """
    Unit tests for the model
    """
    def setUp(self):
        """ Set up method """
        self.user = User()
        self.user.email = 'sebastian.dahlgren@skymill.se'
        self.user.first_name = 'Sebastian'
        self.user.last_name = 'Dahlgren'
        self.user.create_table()
        self.user.save()

    def tearDown(self):
        """ Tear down method """
        self.user.delete_table()

    def test_create_table_hash(self):
        """
        Test that a basic table can be created. Only hash key
        """
        class Test(Model):
            class Meta:
                table_name = 'test'
                dynamodb_local = {
                    'host': DYNAMODB_HOST,
                    'port': DYNAMODB_PORT
                }

            username = StringAttribute('username', hash_key=True)

        test = Test()
        test.username = 'myuser'
        test.create_table()
        key_schema = test.describe_table()['Table']['KeySchema']

        self.assertEqual(len(key_schema), 1)
        self.assertEqual(key_schema[0]['KeyType'], 'HASH')
        self.assertEqual(key_schema[0]['AttributeName'], 'username')

        test.delete_table()

    def test_missing_hash_key(self):
        """
        Check if a missing hash key raises an exception
        """
        class User(Model):
            class Meta:
                table_name = 'test'
                dynamodb_local = {
                    'host': DYNAMODB_HOST,
                    'port': DYNAMODB_PORT
                }

            first_name = StringAttribute('firstName')
            last_name = StringAttribute('lastName')

        user = User()
        self.assertRaises(MissingHashKeyError, user.create_table)


if __name__ == '__main__':
    unittest.main()
