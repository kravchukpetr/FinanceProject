FROM python:3.9
COPY requirements.txt /usr/src/requirements.txt
WORKDIR /usr/src
RUN apt-get update
RUN apt-get install -y cron
RUN apt-get install -y systemd
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
# Add the script to the Docker Image
ADD run_finance.sh /usr/src/run_finance.sh
ADD run_simfin.sh /usr/src/run_simfin.sh
RUN chmod 0775 /usr/src/run_finance.sh
RUN chmod 0775 /usr/src/run_simfin.sh
COPY crontab /etc/cron.d/crontab
RUN crontab /etc/cron.d/crontab
CMD ["cron","-f"]
