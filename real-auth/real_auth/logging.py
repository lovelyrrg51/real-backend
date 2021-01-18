import json
import logging


# https://docs.python.org/3/howto/logging-cookbook.html#using-a-context-manager-for-selective-logging
class LogLevelContext:
    def __init__(self, logger, level):
        self.logger = logger
        self.level = level

    def __enter__(self):
        self.old_level = self.logger.level
        self.logger.setLevel(self.level)

    def __exit__(self, et, ev, tb):
        self.logger.setLevel(self.old_level)


# https://github.com/python/cpython/blob/master/Lib/logging/__init__.py#L510
class CloudWatchFormatter(logging.Formatter):
    "Format logging records so they're json and readable in CloudWatch"

    def format(self, record):
        # clear away the lamba path prefix
        prefix = '/var/task/'
        start = len(prefix) if record.pathname.startswith(prefix) else 0
        path = record.pathname[start:]

        data = {
            'level': record.levelname,
            'event': getattr(record, 'event', None),
            'message': record.getMessage(),
            'sourceFile': path,
            'sourceLine': record.lineno,
        }
        if record.exc_info:
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
            data['exceptionInfo'] = record.exc_text.split('\n')
        if record.stack_info:
            data['stackInfo'] = record.stack_info.split('\n')
        return f'{json.dumps(data)}'


logger = logging.getLogger()
for handler in logger.handlers:
    handler.setFormatter(CloudWatchFormatter())
