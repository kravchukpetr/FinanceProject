#!/bin/bash

if [ "$TERM" = "cygwin" ]; then
  WINPTY_CMD="winpty"
else
  WINPTY_CMD=""
fi

CONTAINER_NAME="ansible-container"
VOLUME_INVENTORY="inventory"
VOLUME_PLAYBOOKS="playbooks"

docker stop $(docker ps -a -q -f name=$CONTAINER_NAME)
[ "$(docker ps -a -q -f name=$CONTAINER_NAME)" ] && docker rm $CONTAINER_NAME || echo "Container '$CONTAINER_NAME' does not exist."
[ "$(docker images -q $CONTAINER_NAME)" ] && docker rmi $CONTAINER_NAME || echo "Image '$CONTAINER_NAME' does not exist."
[ "$(docker volume ls -q -f name=$VOLUME_INVENTORY)" ] && docker volume rm $VOLUME_INVENTORY || echo "Volume '$VOLUME_INVENTORY' does not exist."
[ "$(docker volume ls -q -f name=$VOLUME_PLAYBOOKS)" ] && docker volume rm $VOLUME_PLAYBOOKS || echo "Volume '$VOLUME_INVENTORY' does not exist."
docker build -t $CONTAINER_NAME .
$WINPTY_CMD docker run -d --name $CONTAINER_NAME --env-file .env -v ./playbooks:/ansible/playbooks -v ./inventory:/ansible/inventory $CONTAINER_NAME
