


docker network rm airflow-spark-net
docker network create --driver overlay --attachable airflow-spark-net
docker stack deploy -c docker-compose.yaml scigility

docker service logs scigility_scigility-api --follow
docker swarm init
# List services
docker service ls

# Check all replicas
docker service ps scigility_scigility-api
docker service scale scigility_scigility-api=5
docker stack rm scigility


cd ~/EC2/scigility

git config user.name "engnadermourad"
git config user.email "eng.nader.mourad@gmail.com"
git config user.name
git config user.email



ssh-keygen -t ed25519 -C "eng.nader.mourad@gmail.com" -f ./id_ed25519_scigility
ssh-add /home/ubuntu/EC2/scigility/id_ed25519_scigility
eval "$(ssh-agent -s)"
ssh-add /home/ubuntu/EC2/scigility/id_ed25519_scigility
ssh-add -l
ssh -T git@github.com
git remote set-url origin git@github.com:engnadermourad/scigility-api.git
git remote -v
git push -u origin main
