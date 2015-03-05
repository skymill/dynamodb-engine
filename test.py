from dynamodb_engine.models import Model
from dynamodb_engine.attributes import (
    StringAttribute,
    NumberAttribute
)


class User(Model):
    class Meta:
        table_name = 'users'
        region = 'eu-west-1'
        dynamodb_local = {
            'host': 'localhost',
            'port': 8000
        }
        throughput = {
            'write': 2
        }

    email = StringAttribute('email', hash_key=True)
    first_name = StringAttribute('firstName')
    last_name = StringAttribute('lastName')
    age = NumberAttribute('age')

user = User()
user.create_table(recreate=True)
user.email = 's@d.c'
user.first_name = 'Sebastian'
user.age = 42
user.save()
print(user.first_name)
print(type(user.first_name))
print(user.age)
print(type(user.age))
