import jsonschema, json, os

SCHEMA = json.load(file(os.path.join(os.path.dirname(__file__),'schema.json')))


class Validator(object):

    def __init__(self,script):
        self.script = script

    def validate(self):
        jsonschema.validate(self.script,SCHEMA)