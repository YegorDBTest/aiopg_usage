#!/bin/bash

(python src/main.py) &

# while true; do
# 	sleep 60
# done

python src/manage.py runserver 0:8000
