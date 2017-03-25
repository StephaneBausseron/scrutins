FROM ubuntu:16.04

RUN apt update && apt install -y apt-utils && apt dist-upgrade -y
RUN apt install -yyq build-essential python2.7 basex libsaxonb-java inkscape default-jre vim git nginx python-pip curl
COPY nginx-site /etc/nginx/sites-enabled/default
COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

VOLUME /workdir
WORKDIR /workdir

