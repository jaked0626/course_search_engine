#!/usr/bin/env python
import os
import sys
import random

if __name__ == "__main__":
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ui.settings")
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    argv = sys.argv[:]
    if len(argv) > 1 and argv[1] == "go":
        argv = [argv[0], "runserver",
                "127.0.0.1:" + str(random.randrange(22000, 23000))]
    execute_from_command_line(argv)
