from marshmallow import Schema,fields,validate
class StartScanSchema(Schema):
    scanId=fields.Str(required=True,validate=validate.Length(min=1,max=64)); organizationId=fields.Str(required=True,validate=validate.Length(min=1,max=128)); type=fields.Str(load_default='deals'); auth=fields.Dict(required=True); filters=fields.Dict(load_default=dict)
    def validate_auth(self,data):
        if not data.get('accessToken'): raise ValueError('auth.accessToken is required')
class PaginationSchema(Schema):
    organizationId=fields.Str(required=False); limit=fields.Int(load_default=20,validate=validate.Range(min=1,max=100)); offset=fields.Int(load_default=0,validate=validate.Range(min=0))
