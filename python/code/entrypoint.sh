#!/bin/bash

(python src/manage.py messages_handle_app) &

# while true; do
# 	sleep 60
# done

python src/manage.py runserver 0:8000
