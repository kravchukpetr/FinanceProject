FROM python:3.8
WORKDIR /opt/app
COPY requirements.txt /opt/app/requirements.txt
COPY ./FinanceLib.py /opt/app/FinanceLib.py
COPY ./RunDailyFinance.py /opt/app/RunDailyFinance.py
COPY app.py /opt/app/app.py
COPY ./DB.config /opt/app/DB.config
RUN apt-get update
RUN apt-get install -y cron
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
COPY crontab /etc/cron.d/crontab
RUN crontab /etc/cron.d/crontab
CMD ["cron","-f", "-l", "2"]
