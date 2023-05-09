import logging
import signal
import sys

from blockchainetl.jobs.exporters.console_item_exporter import ConsoleItemExporter
from blockchainetl.logging_utils import logging_basic_config


def get_item_exporter(output):
    if output is None:
        return ConsoleItemExporter()

    from blockchainetl.jobs.exporters.google_pubsub_item_exporter import GooglePubSubItemExporter
    return GooglePubSubItemExporter(
        item_type_to_topic_mapping={
            'block': f'{output}.blocks',
            'transaction': f'{output}.transactions',
            'log': f'{output}.logs',
            'token_transfer': f'{output}.token_transfers',
            'trace': f'{output}.traces',
            'contract': f'{output}.contracts',
            'token': f'{output}.tokens',
        }
    )


def configure_signals():
    def sigterm_handler(_signo, _stack_frame):
        # Raises SystemExit(0):
        sys.exit(0)

    signal.signal(signal.SIGTERM, sigterm_handler)


def configure_logging(filename):
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logging_basic_config(filename=filename)
