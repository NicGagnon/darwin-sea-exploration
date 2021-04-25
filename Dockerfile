FROM python:3.7-buster

# NEEDED FOR JAVA
RUN mkdir -p /usr/share/man/man1

RUN apt-get update && apt-get install -y \
	build-essential \
    openjdk-11-jre

WORKDIR /home/site/wwwroot

COPY Pipfile Pipfile
COPY Pipfile.lock Pipfile.lock
RUN pip install pipenv
RUN pipenv install --dev --deploy --system

COPY . /home/site/wwwroot