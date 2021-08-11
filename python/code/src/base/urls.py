from django.urls import path

from test.views import send_message


urlpatterns = [
    path('send_message/<int:message_id>/', send_message, name='send_message'),
]
