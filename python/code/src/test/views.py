import logging

from django.http import HttpResponse


def send_message(request, message_id):
    logging.warning(f'SERVER message_id {message_id}')
    return HttpResponse()
