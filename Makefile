build:
	docker-compose -f docker-compose.yaml up -d --build 
	
build-nocache:
	docker-compose -f docker-compose.yaml up -d --build --no-cache

buildrm:
	docker-compose -f docker-compose.yaml up -d --build --remove-orphans

up:
	docker-compose -f docker-compose.yaml up -d

down:
	docker-compose -f docker-compose.yaml down

run:
	make down && make up

downv:
	docker-compose -f docker-compose.yaml down -v

# Docker Swarm Commands
swarm-init:
	docker swarm init

stack-deploy:
	docker stack deploy -c docker-compose.yaml scigility

stack-rm:
	docker stack rm scigility

service-ls:
	docker service ls

service-logs:
	docker service logs scigility_scigility-api --follow

service-ps:
	docker service ps scigility_scigility-api

service-scale:
	docker service scale scigility_scigility-api=5