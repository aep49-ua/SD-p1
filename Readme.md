# WaterManagement

Práctica de Sistemas Distribuidos (curso 2026/27, Universidad de Alicante), hecha por Alejandro Prats y Arnau Escriva.

Es un sistema de riego con tres programas en Java: una central, unas estaciones de riego y unos operarios de campo. Las estaciones hablan con la central por sockets, los operarios y las estaciones usan Kafka para las peticiones y los resultados, y la central guarda todo en una base de datos SQLite y lo enseña en un panel web.

## Carpetas

- `WM_Central`: la central. Atiende a las estaciones por sockets (puerto 5000), consume de Kafka, guarda en SQLite y tiene el panel web en el puerto 8080.
- `WM_WS`: la estación de riego. Se compone de `WM_WS_M` (el monitor, que habla con la central) y `WM_WS_E` (el engine, que simula la electroválvula).
- `WM_FO`: el operario de campo, que pide riegos por consola.
- `aws/`: lo necesario para levantar Kafka y la central en una EC2.
- `compose.yml`: Kafka y la central compilados desde el código, para probar en local.
- `.github/workflows/deploy.yml`: construye las cuatro imágenes y las sube a Docker Hub.
- `prompts/`: prompts que hemos usado con IA durante el desarrollo.

## Imágenes de Docker Hub

Están publicadas (públicas) y es lo que se usa para desplegar, sin compilar nada:

- `alejandropraats/wm-central`
- `alejandropraats/wm-ws-m` (monitor)
- `alejandropraats/wm-ws-e` (engine)
- `alejandropraats/wm-fo`

Las construye el workflow cada vez que se hace push a `main`. Para que funcione hay que tener definidos los secrets `DOCKER_USERNAME` y `DOCKER_PASSWORD` (un access token de Docker Hub) en el repositorio de GitHub.

## Ponerlo en marcha

Cada programa va en su propia terminal y se deja abierta mientras se usa. Los comandos están en PowerShell; en Linux o Mac se escribe `export VARIABLE=valor`.

### 1. Kafka y la central

En AWS se levantan con el script de `aws/` (ver más abajo). Para probar todo en el mismo ordenador, desde la raíz del repositorio:

```
$env:KAFKA_HOST="host.docker.internal"
$env:DOCKER_USER="alejandropraats"
docker compose -f aws/compose.aws.yml up -d
```

El panel se abre en http://localhost:8080. La base de datos se crea sola la primera vez, no va en el repositorio.

### 2. Una estación

En la carpeta `WM_WS`, con la dirección de la central y los datos de la estación (en local, `host.docker.internal`; en AWS, la IP pública):

```
cd WM_WS
$env:CENTRAL_HOST="host.docker.internal"
$env:BROKER_HOST="host.docker.internal"
$env:WS_ID="WS-01"
$env:WS_UBICACION="River Park"
docker compose up monitor
```

Y en otra terminal, con las mismas variables definidas, el engine, que es interactivo:

```
docker compose run --rm engine
```

En el panel la estación pasa a DISPONIBLE. El menú del engine es este:

- `r <segundos>`: pide un riego manual (lo autoriza la central).
- `s`: termina el riego en curso.
- `k`: simula una avería; si se vuelve a pulsar, la resuelve.
- `e`: enseña el estado de la estación.
- `q`: sale.

Para levantar otra estación a la vez se repite con otro `WS_ID` y otra ubicación, y un nombre de proyecto distinto para que no choque con la primera, por ejemplo `docker compose -p ws02 up monitor` y `docker compose -p ws02 run --rm engine`.

### 3. Un operario

```
docker run -it --rm alejandropraats/wm-fo:latest host.docker.internal 9092 FO-01
```

Los argumentos son la IP del broker, el puerto (9092) y el id del operario. Los operarios que existen en la base de datos son `FO-01`, `FO-05` y `FO-12`. Una vez dentro:

- `WS-01#20` pide un riego de 20 segundos en `WS-01`.
- `lista` enseña las estaciones.
- `salir` cierra el operario.

Si se pasa un cuarto argumento con un fichero de peticiones (una por línea, `estacion#segundos`), las lanza una detrás de otra. La imagen trae uno de ejemplo en `/app/servicios.txt`.

### Desde el panel y desde la consola de la central

En el panel se elige una estación, se pone la duración y se pulsa Iniciar riego, Bloquear WS o Activar WS. La central también acepta por consola `START#<ws>#<segundos>`, `BLOCK#<ws>` y `ACTIVATE#<ws>` (con `TODAS` se aplica a todas las estaciones).

### Ver logs y parar contenedores

El nombre de cada contenedor sale con `docker ps`. Con ese nombre:

```
docker logs -f <nombre>
docker stop <nombre>
docker start <nombre>
```

Para quitar todo lo de la central y Kafka: `docker compose -f aws/compose.aws.yml down`. Para quitar una estación: salir del engine y `docker compose down` en `WM_WS`.

## Ejecutar sin Docker (para desarrollar)

Hace falta Java 17 y Maven. Kafka sí tiene que estar en marcha, y para eso basta con `docker compose up -d broker` desde la raíz. Después, cada uno en su terminal:

```
cd WM_Central && mvn -q package && java -cp target/app.jar WM_Central 5000 localhost 9092 8080
cd WM_WS && mvn -q package && java -cp target/app.jar WM_WS_M 6001 localhost 5000 WS-01 "River Park"
cd WM_WS && java -cp target/app.jar WM_WS_E localhost 9092 localhost 6001
cd WM_FO && mvn -q package && java -cp target/app.jar WM_FO localhost 9092 FO-01
```

## Despliegue en AWS

Se usa una EC2 y no un servicio gestionado porque la central necesita aceptar conexiones TCP en el 5000 (los sockets de las estaciones) además del panel HTTP.

1. Subir el repositorio a GitHub y definir los secrets `DOCKER_USERNAME` y `DOCKER_PASSWORD`. El workflow publica las cuatro imágenes en Docker Hub.
2. Crear un grupo de seguridad con entrada TCP para los puertos 22, 5000, 8080 y 9092.
3. Lanzar una instancia Ubuntu 22.04 con ese grupo de seguridad y pegar `aws/user-data.sh` en los datos de usuario. Antes hay que cambiar dentro `DOCKER_USER` (el usuario de Docker Hub) y el usuario y el repositorio de GitHub de la ruta del compose.
4. El script instala Docker, lee la IP pública de la instancia, la guarda como `KAFKA_HOST` (es la dirección que Kafka anuncia a los clientes) y levanta Kafka y la central con `aws/compose.aws.yml`.
5. Pasados un par de minutos, el panel está en `http://<ip_publica>:8080`, con las cuatro estaciones en DESCONECTADA.
6. En los ordenadores del laboratorio se arrancan las estaciones y los operarios como arriba, pero con la IP pública en `CENTRAL_HOST`, `BROKER_HOST` y en el comando del operario.

Si la instancia se para y al volver a arrancar tiene otra IP pública, hay que recrearla con el mismo script, porque `KAFKA_HOST` se fija al arrancar. Al terminar hay que parar o terminar la instancia para no gastar crédito del laboratorio.
