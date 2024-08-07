export $(grep -v '^#' /usr/src/app/.env | xargs)
/usr/local/bin/python /usr/src/app/RunDailyFinance.py