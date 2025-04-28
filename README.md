## Linux commands
### check folder mostly used disk space
```bash
du -h --max-depth=1 | sort -hr | head -n 10
```
### clear journal history logs
```bash
journalctl --vacuum-time=1d
```
### check crontab
```bash
crontab -l
```



## Docker commands
### собраить и запустить контейнеры
```bash
docker-compose up -d
```
### посмотреть список контейнеров
```bash
docker-compose ps
docker ps -a
```
### остановить контейнеры
```bash
docker-compose stop
```
### запустить контейнеры
```bash
docker-compose start
```
### зайти в контейнер
```bash
docker exec -it conainer_id /bin/bash
```
### остановка всех запущенных контейнеров
```bash
docker stop $(docker ps --filter status=running -q)
```
### удаление всех контейнеров
```bash
docker rm $(docker ps --filter status=exited -q)
```
### удаление всех образов
```bash
docker rmi $(docker images -q)
```
### удаление всех volume
```bash
docker volume rm $(docker volume ls -q)
```
### all unused data
```bash
docker system prune
```
### stop and remove all containers
```bash
docker-compose down
```
### stop and remove all containers with named volumes
```bash
docker-compose down -v
```
### stop containers and remove all images
```bash
docker-compose down --rmi all
```
### ensure docker service is running
```bash
sudo systemctl status docker
```



## Backup PG
### backup
```bash
docker exec -t postgres_finance pg_dump -U postgres -d postgres -f /var/lib/postgresql/data/backup_finance.sql
```
### copy backup from container to remote server directory
```bash
docker cp postgres_finance:/var/lib/postgresql/data/backup_finance.sql /projects/backup/backup_finance.sql
```
## pgAdmin
http://89.253.222.32:5050/browser/

## Restore PG from backup

```bash
docker cp /projects/backup/backup-2024-08-06.sql f323f072f588:/backup-2024-08-06.sql
docker exec -it f323f072f588 bin/bash
psql -U postgres -d postgres -f ./backup-2024-08-06.sql

psql -U postgres -d postgres -f /backup-2024-08-06.sql
/usr/lib/postgresql/15/bin/pg_resetwal /var/lib/postgresql/data
docker run -it --rm --entrypoint bash financeproject-postgres
```
