""" Testing the Model """
import unittest

from .attributes import StringAttribute
from .connection import connect
from .exceptions import (
    MissingHashKeyError,
    TableDoesNotExistError,
    ValidationError)
from .models import Model

DYNAMODB_HOST = 'localhost'
DYNAMODB_PORT = 8000

CONNECTION = connect(
    dynamodb_local_host=DYNAMODB_HOST,
    dynamodb_local_port=DYNAMODB_PORT)


class User(Model):
    """ Basic User model used for testing purposes """
    class Meta:
        table_name = 'users'
        throughput = {
            'read': 22,
            'write': 18
        }

    email = StringAttribute('email', hash_key=True)
    first_name = StringAttribute('firstName')
    last_name = StringAttribute('lastName')


class TestModelCreateTable(unittest.TestCase):
    """ Unit tests for the Model.create_table() """
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

    def test_create_table_name(self):
        """ Ensure that tables get the right name """
        self.assertEqual(
            self.user.describe_table()['Table']['TableName'],
            'users')

    def test_create_table_throughput(self):
        """ Ensure that the throughput is provisioned properly """
        cus = self.user.describe_table()['Table']['ProvisionedThroughput']
        self.assertEqual(cus['ReadCapacityUnits'], 22)
        self.assertEqual(cus['WriteCapacityUnits'], 18)

    def test_create_table_hash(self):
        """ Test that a basic table can be created. Only hash key """
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

    def test_create_table_hash_range(self):
        """
        Test that a basic table can be created. Both hash key and range key
        """
        class Test(Model):
            class Meta:
                table_name = 'test'
                dynamodb_local = {
                    'host': DYNAMODB_HOST,
                    'port': DYNAMODB_PORT
                }

            username = StringAttribute('username', hash_key=True)
            post = StringAttribute('post', range_key=True)

        test = Test()
        test.username = 'myuser'
        test.post = 'example'
        test.create_table()
        key_schemas = test.describe_table()['Table']['KeySchema']

        self.assertEqual(len(key_schemas), 2)
        for key_schema in key_schemas:
            if key_schema['KeyType'] == 'HASH':
                self.assertEqual(key_schema['AttributeName'], 'username')
            if key_schema['KeyType'] == 'RANGE':
                self.assertEqual(key_schema['AttributeName'], 'post')

        test.delete_table()

    def test_missing_hash_key(self):
        """ Check if a missing hash key raises an exception """
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

    def test_validation_error(self):
        """ Test to ignore a required key """
        class Test(Model):
            class Meta:
                table_name = 'test'
                dynamodb_local = {
                    'host': DYNAMODB_HOST,
                    'port': DYNAMODB_PORT
                }

            username = StringAttribute('username', hash_key=True)
            post = StringAttribute('post', range_key=True)

        test = Test()
        test.create_table()
        test.username = 'myuser'
        self.assertRaises(ValidationError, test.save)
        test.delete_table()


class TestModelDeleteTable(unittest.TestCase):
    """ Test table deletion for models """
    def test_delete_table(self):
        """ Try create and delete on a table """
        self.assertNotIn('users', CONNECTION.list_tables()['TableNames'])

        user = User()
        user.create_table()
        self.assertIn('users', CONNECTION.list_tables()['TableNames'])

        user.delete_table()
        self.assertNotIn('users', CONNECTION.list_tables()['TableNames'])

    def test_delete_table_that_does_not_exist(self):
        """ Try to remove a table that does not exist """
        self.assertNotIn('users', CONNECTION.list_tables()['TableNames'])

        user = User()
        self.assertRaises(TableDoesNotExistError, user.delete_table)


class TestModelDescribeTable(unittest.TestCase):
    """ Unit tests for the Model.describe_table() """
    def setUp(self):
        """ Set up method """
        self.user = User()
        self.user.create_table()

    def tearDown(self):
        """ Tear down method """
        self.user.delete_table()

    def test_describe_table(self):
        """ Test that describe_table() gives you a table description """
        desc = self.user.describe_table()
        self.assertIn('TableStatus', desc['Table'].keys())
        self.assertIn('ProvisionedThroughput', desc['Table'].keys())
        self.assertIn(
            'ReadCapacityUnits',
            desc['Table']['ProvisionedThroughput'].keys())
        self.assertIn(
            'WriteCapacityUnits',
            desc['Table']['ProvisionedThroughput'].keys())
        self.assertIn(
            'LastIncreaseDateTime',
            desc['Table']['ProvisionedThroughput'].keys())
        self.assertIn(
            'LastDecreaseDateTime',
            desc['Table']['ProvisionedThroughput'].keys())
        self.assertIn(
            'NumberOfDecreasesToday',
            desc['Table']['ProvisionedThroughput'].keys())
        self.assertIn('ItemCount', desc['Table'].keys())
        self.assertIn('KeySchema', desc['Table'].keys())
        self.assertEqual(len(desc['Table']['KeySchema']), 1)
        self.assertEqual(
            desc['Table']['KeySchema'][0]['KeyType'],
            'HASH')
        self.assertEqual(
            desc['Table']['KeySchema'][0]['AttributeName'],
            'email')
        self.assertIn('AttributeDefinitions', desc['Table'].keys())
        self.assertIn('CreationDateTime', desc['Table'].keys())
        self.assertIn('TableSizeBytes', desc['Table'].keys())

    def test_describe_non_existing_table(self):
        """ Try to describe a non existing table """
        class Test(Model):
            class Meta:
                table_name = 'something'
                dynamodb_local = {
                    'host': DYNAMODB_HOST,
                    'port': DYNAMODB_PORT
                }

        test = Test()
        self.assertRaises(TableDoesNotExistError, test.describe_table)
