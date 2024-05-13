#!/usr/bin/env python3

from prettyconf import config

DEBUG = config('DEBUG', cast=config.boolean, default=False)

DEFAULT_SINCE_DAYS = config('MADROX_SINCE_DAYS', cast=int, default=7)
