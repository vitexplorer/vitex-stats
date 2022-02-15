FROM python:3.8-slim

ENV FLASK_APP=vitex_stats_server
ENV FLASK_ENV=production
ENV FLASK_CONFIG=/app/prod_config.py

RUN mkdir /app

WORKDIR /app

ADD requirements.txt prod_config.py wsgi.py uwsgi.ini start.sh /app/
ADD vitex_stats_server /app/vitex_stats_server

RUN apt-get clean && \
    apt-get update && \
    apt-get install -y python3-dev build-essential nginx libpq-dev libpcre3 libpcre3-dev && \
    pip install -r requirements.txt

COPY nginx.conf /etc/nginx


RUN chmod 755 start.sh
CMD ["./start.sh"]