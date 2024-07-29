FROM postgres:15
RUN apt-get update
RUN apt-get -y install pgagent
COPY init.sql /docker-entrypoint-initdb.d/
