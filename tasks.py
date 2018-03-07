from celery import Celery
from celery.schedules import crontab
import logging
from pythonjsonlogger import jsonlogger
from celery.signals import setup_logging
from datetime import datetime
import re


#app = Celery()
app = Celery('tasks', backend='db+mysql://celery:celery123@0.0.0.0:3308/celerydb', broker='pyamqp://guest@0.0.0.0:5672//')

# update configuration
app.conf.update(timezone='America/Chicago')

logger = logging.getLogger('celery')


# custom formatter since default log format is just [format = %(message)s]
class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super(CustomJsonFormatter, self).add_fields(log_record, record,
                                                    message_dict)
        if not log_record.get('timestamp'):
            # this doesn't use record.created, so it is slightly off
            now = datetime.utcnow()
            fmt_now = now.strftime("%Y-%m-%dT%H:%M:%S") + ".%03d" % (
                now.microsecond / 1000) + "Z"
            log_record['@timestamp'] = fmt_now
            # removing 'timestamp' as we want '@timestamp' for kibana
            timestamp = log_record.pop('timestamp', None)
        if log_record.get('level'):
            log_record['level'] = log_record['level'].upper()
        else:
            log_record['level'] = record.levelname


formatter = CustomJsonFormatter(
    '(timestamp) (level) (name) (message)')

# override celery logger with json-logger
def setup_json_logging(loglevel=logging.INFO, **kwargs):
    logHandler = logging.StreamHandler()
    logHandler.setFormatter(formatter)
    logger.addHandler(logHandler)
    logger.setLevel(loglevel)
    return logger

# signal celery to override logger setup
setup_logging.connect(setup_json_logging)

@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):

    # Calls test('hello') every 10 seconds.
    sender.add_periodic_task(10.0, test.s('hello'), name='add every 10')

    # Calls test('world') every 30 seconds
    sender.add_periodic_task(30.0, test.s('world'), expires=10)

    # Executes every Monday morning at 7:30 a.m.
    sender.add_periodic_task(
        crontab(minute="*"),
        test.s('Happy Mondays!'),
    )

@app.task
def test(arg):
    print(arg)
