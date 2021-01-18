from app.handlers.appsync import routes


@routes.register('Type.field1')
def handler_1(caller_user_id, arguments, source, context):
    pass


@routes.register('Type.field2')
def handler_2(caller_user_id, arguments, source, context):
    pass
