FROM python:3.9.6

ADD ./requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip && \
	  pip install --no-cache-dir -r /tmp/requirements.txt

ADD ./code /app
WORKDIR /app

CMD ["python", "src/main.py"]
