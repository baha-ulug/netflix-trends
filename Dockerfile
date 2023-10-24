FROM amazon/aws-lambda-python:3.9

ENV PYTHONUNBUFFERED True

RUN yum -y install tar
RUN yum -y install gzip

COPY . /opt/app
WORKDIR /opt/app
RUN pip install -r requirements.txt
