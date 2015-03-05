"""
Testing the Model
"""
import unittest

from .attributes import StringAttribute
from .exceptions import MissingHashKeyError
from .models import Model

DYNAMODB_HOST = 'localhost'
DYNAMODB_PORT = 8000


class TestModel(unittest.TestCase):
    """
    Unit tests for the model
    """
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
    unittest.main(verbosity=2)
