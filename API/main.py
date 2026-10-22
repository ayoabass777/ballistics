"""ASGI entrypoint compatibility module.

Use ``API.main:app`` for runtimes that expect a conventional main module.
"""

from API.app import app
