FROM python:3.8
COPY requirements.txt /usr/src/app/requirements.txt
COPY ../FinanceLib.py /usr/src/app/FinanceLib.py
COPY ./RunDailyFinance.py /usr/src/app/RunDailyFinance.py
COPY ./DB.config /usr/src/app/DB.config
WORKDIR /usr/src/app
RUN apt-get update
RUN apt-get install -y cron
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
COPY crontab /etc/cron.d/crontab
RUN crontab /etc/cron.d/crontab
CMD ["cron","-f", "-l", "2"]
