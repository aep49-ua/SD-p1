# WaterManagement

Práctica de Sistemas Distribuidos (Universidad de Alicante). Sistema de riego con sockets, Kafka, SQLite y despliegue en AWS.

## Estructura

- `WM_Central`: servidor central (sockets con las estaciones, consumidor Kafka, SQLite, panel web en el puerto 8080).
- `WM_WS`: estación de riego. `WM_WS_M` (monitor) y `WM_WS_E` (engine).
- `WM_FO`: operario de campo.
- `compose.yml`: entorno local (Kafka + Central).
- `aws/`: despliegue en EC2.
- `.github/workflows/deploy.yml`: construye y sube las tres imágenes a Docker Hub.

## Ejecución local

```
docker compose up -d --build
```

Panel web: http://localhost:8080

Estación (otra terminal):

```
cd WM_WS
CENTRAL_HOST=host.docker.internal BROKER_HOST=host.docker.internal docker compose up -d monitor
docker compose run --rm engine
```

Operario:

```
cd WM_FO
docker build -t wm-fo .
docker run -it --rm wm-fo <ip_broker> <puerto_broker> <id_operador> [fichero]
```

## Ejecución sin Docker

```
cd WM_Central && mvn -q package && java -cp target/app.jar WM_Central 5000 localhost 9092 8080
cd WM_WS && mvn -q package && java -cp target/app.jar WM_WS_M 6001 localhost 5000 WS-01 "River Park"
cd WM_WS && java -cp target/app.jar WM_WS_E localhost 9092 localhost 6001
cd WM_FO && mvn -q package && java -cp target/app.jar WM_FO localhost 9092 FO-01
```

## Despliegue en AWS

Kafka y Central se ejecutan en una instancia EC2 con Docker, porque Central necesita un puerto TCP (5000) además del panel HTTP.

1. Subir el repositorio a GitHub y definir los secrets `DOCKER_USERNAME` y `DOCKER_PASSWORD`.
2. El workflow publica `wm-central`, `wm-ws` y `wm-fo` en Docker Hub.
3. Lanzar una EC2 Ubuntu con `aws/user-data.sh` como user data (editar usuario de Docker Hub y de GitHub).
4. Security group: 22, 9092, 5000 y 8080.
5. Panel: `http://<ip_publica>:8080`.
6. En los PCs del laboratorio, usar la IP pública para `CENTRAL_HOST` y `BROKER_HOST`.
