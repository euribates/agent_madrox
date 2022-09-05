from prettyconf import config

DEBUG = config('DEBUG', default=False, cast=config.boolean)
