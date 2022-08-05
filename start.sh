#!/bin/bash

gunicorn -w 8 main:app -b 0:8000 --error-logfile /home/merlot/gunicorn_error.log --timeout=3000

