import logging
import pprint
from pathlib import Path

# import settings

DEFAULT_LOG_LEVEL = logging.INFO  # 20


def validate_log_path(log_file):
    if Path(log_file).parent is None:
        raise Exception(f"Log file path not found: {Path(log_file).parent}")


# Define a custom formatter
class PPrintFormatter(logging.Formatter):
    def format(self, record):
        # Use pprint.pformat to pretty print the record message
        record.msg = pprint.pformat(record.msg, indent=2)
        return super().format(record)

# Define a custom formatter
class DefaultFormatter(logging.Formatter):
    def format(self, record):
        # Use pprint.pformat to pretty print the record message
        record.msg = pprint.pformat(record.msg, indent=2)
        return super().format(record)


def get_logger(logger_name="main_logger", log_file=None, log_level=DEFAULT_LOG_LEVEL):
    formatter = logging.Formatter('[%(asctime)s][%(levelname)5s][%(name)s][%(lineno)3d][%(filename)s]:%(message)s')

    existing_logger = logging.getLogger(logger_name)
    handler_types_list = [type(_) for _ in existing_logger.handlers]

    if logging.StreamHandler in handler_types_list:
        existing_logger.debug(f"Reusing Logger {existing_logger.name}")

    else:
        print("Creating logger")
        logger = logging.getLogger(logger_name)

        # Create console handler for logger.
        ch = logging.StreamHandler()
        ch.setLevel(level=log_level)
        ch.setFormatter(formatter)
        # ch.setFormatter(PPrintFormatter())
        logger.addHandler(ch)

    if log_file is not None and logging.FileHandler not in handler_types_list:
        validate_log_path(log_file)
        fh = logging.FileHandler(log_file)
        fh.setLevel(level=log_level)
        fh.setFormatter(formatter)
        existing_logger.info("Adding File handler to logger")
        existing_logger.addHandler(fh)

    existing_logger.setLevel(log_level)
    return existing_logger


# if __name__ == '__main__':
#     main_logger1 = get_logger()
#     log_path = Path(settings.BASE_PATH).joinpath("step_id.log").as_posix()
#     main_logger1.info("test")
#     main_logger1 = get_logger(log_file=log_path)
#     main_logger1.info("test after file handler")
