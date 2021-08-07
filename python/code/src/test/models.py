from django.db import models


class Message(models.Model):
    done = models.BooleanField(default=False)
    sending = models.BooleanField(default=False)
