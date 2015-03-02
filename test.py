from dynamodb_engine.models import Model


class User(Model):
    class Meta:
        region = 'eu-west-1'

    first_name = ''
    last_name = ''

user = User()
