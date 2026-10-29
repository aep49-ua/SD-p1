#!/bin/bash
set -e
DOCKER_USER="CAMBIAR_USUARIO_DOCKERHUB"
REPO_RAW="https://raw.githubusercontent.com/CAMBIAR_USUARIO_GITHUB/CAMBIAR_REPO/main/aws/compose.aws.yml"

apt-get update -y
apt-get install -y docker.io docker-compose-v2 curl
systemctl enable --now docker

TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 300")
KAFKA_HOST=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/public-ipv4)

mkdir -p /opt/wm
cd /opt/wm
curl -sSL "$REPO_RAW" -o compose.yml
printf "KAFKA_HOST=%s\nDOCKER_USER=%s\n" "$KAFKA_HOST" "$DOCKER_USER" > .env
docker compose up -d
