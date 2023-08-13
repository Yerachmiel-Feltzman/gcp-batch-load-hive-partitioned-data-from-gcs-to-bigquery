import logging
import os

APP_LOG_LEVEL_ENV_VAR = "APP_LOG_LEVEL"


def get_log_level_from_env() -> str:
    level_input = os.environ.get(APP_LOG_LEVEL_ENV_VAR, "INFO")
    return level_input.upper()


def set_log_level_from_env(force: bool = False):
    level = get_log_level_from_env()
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % level)

    logging.basicConfig(level=numeric_level, force=force)
