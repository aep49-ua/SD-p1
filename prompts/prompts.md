## 1. Diseño de la arquitectura

1. Tengo un Central que habla por sockets con los monitores de las estaciones y por Kafka con los motores y los operarios. ¿Por qué el enunciado separa los dos canales? Explícame qué ventajas tiene el acoplamiento débil de Kafka frente a sockets para telemetría y órdenes.
   *Objetivo: justificar la elección de tecnología en la defensa.*
2. Si WM_Central se cae durante un riego, ¿qué debería seguir funcionando y qué no? Dame los criterios de resiliencia, sin código, y yo los contrasto con mi diseño.
   *Objetivo: razonar sobre fallos parciales.*
3. ¿Qué información debe guardar la base de datos de Central y cuál conviene calcular al vuelo? Explícame el criterio para decidir si el estado de una estación va en SQLite o solo en memoria.
   *Objetivo: modelado de datos y fuente de la verdad.*

## 2. Sockets y protocolo

4. Explícame cómo funciona el LRC como XOR byte a byte y qué errores detecta y cuáles no. ¿Por qué se usa ACK/NACK además del LRC?
   *Objetivo: entender el protocolo `<STX><DATA><ETX><LRC>`.*
5. Mi servidor lee con `readUTF` y a veces se bloquea esperando. ¿Qué diferencia hay entre un timeout de lectura (`setSoTimeout`) y detectar que el cliente ha cerrado la conexión? Dame pistas para depurarlo, sin darme la solución completa.
   *Objetivo: gestión de lecturas bloqueantes y detección de caídas.*
6. ¿Qué pasa en TCP cuando un proceso se mata de golpe (Ctrl+C) frente a cuando se desconecta el cable? ¿Cómo se manifiesta cada caso en el servidor?
   *Objetivo: distinguir cierre limpio, reset y silencio.*
7. Quiero que el monitor envíe un latido cada pocos segundos. ¿Qué compromiso hay entre el intervalo del latido y el tiempo máximo para declarar desconectada una estación?
   *Objetivo: dimensionar timeouts.*

## 3. Concurrencia

8. Central crea un hilo por conexión. Si dos hilos actualizan la misma estación a la vez, ¿qué condiciones de carrera pueden aparecer? Dame ejemplos de intercalado, no la corrección.
   *Objetivo: razonar sobre races en la capa de datos.*
9. Una estación se reconecta mientras el hilo antiguo aún no ha detectado que murió. ¿Cómo puedo evitar que el hilo viejo marque la estación como desconectada cuando ya está viva? ¿Qué patrones existen?
   *Objetivo: idea de "propietario" de la conexión.*
10. Revisa de forma crítica este fragmento de mi `HiloWM` y dime qué podría salir mal bajo carga. No lo reescribas: señala los problemas y yo los corrijo.
    *Objetivo: revisión defensiva.*

## 4. Kafka

11. Explícame qué es un consumer group y qué ocurre con los mensajes cuando un consumidor se reinicia. ¿Por qué el operario necesita un grupo estable y la estación uno aleatorio?
    *Objetivo: offsets, grupos y recuperación de mensajes perdidos.*
12. Usé `auto.offset.reset=latest`. ¿En qué casos pierdo mensajes y cuándo me conviene `earliest`?
    *Objetivo: semántica de entrega al arrancar.*
13. Quiero descartar peticiones de riego antiguas cuando Central vuelve de una caída. ¿Qué diferencia hay entre `CreateTime` y `LogAppendTime`, y por qué importa si los relojes de las máquinas no están sincronizados?
    *Objetivo: relojes en sistemas distribuidos.*
14. El productor se queda esperando minutos si el broker no responde. ¿Qué parámetros acotan ese bloqueo (`max.block.ms`, `delivery.timeout.ms`, `request.timeout.ms`) y cómo se relacionan entre sí?
    *Objetivo: mostrar errores controlados al usuario.*

## 5. SQLite

15. ¿Por qué SQLite bloquea con "database is locked" cuando varios hilos escriben? ¿Qué hace `busy_timeout` y qué límites tiene?
    *Objetivo: concurrencia en SQLite.*
16. Explícame cómo funciona un upsert con `ON CONFLICT DO UPDATE` y por qué es útil para registrar estaciones que ya existen.
    *Objetivo: idempotencia del registro.*

## 6. Docker y despliegue

17. Mi Dockerfile compila con Maven y luego copia el jar a una imagen más pequeña. Explícame las ventajas de las construcciones multi-etapa y qué hace el plugin shade en el jar final.
    *Objetivo: justificar el empaquetado.*
18. Dentro de un contenedor, `localhost` no apunta a mi PC. Explícame cómo se resuelven los nombres entre contenedores en Compose y cuándo hay que usar la IP pública o `host.docker.internal`.
    *Objetivo: redes de Docker.*
19. Kafka anuncia una dirección (`advertised.listeners`) distinta de la que escucha. ¿Por qué un cliente remoto falla si anuncia `localhost`? ¿Qué debe anunciar en AWS?
    *Objetivo: configuración de red de Kafka.*

## 7. AWS y CI/CD

20. Mi instancia EC2 no responde desde fuera. Dame una lista ordenada de comprobaciones (security group, IP pública, tabla de rutas, user data) para localizar el fallo.
    *Objetivo: método de diagnóstico de red.*
21. Quiero que GitHub Actions construya imágenes y las suba a Docker Hub. Explícame el papel de los secretos del repositorio y por qué no debo escribir credenciales en el workflow.
    *Objetivo: seguridad básica de CI/CD.*

## 8. Git y trabajo en equipo

22. He tenido un conflicto de merge entre mi rama y `develop`. Explícame cómo leer las marcas `<<<<<<<` y qué criterios aplicar para decidir qué versión conservar cuando el archivo es de mi compañero.
    *Objetivo: resolución consciente de conflictos.*
23. ¿Por qué es mejor `git revert` que reescribir historia con `reset` y `force push` en una rama compartida?
    *Objetivo: seguridad del historial colectivo.*

## 9. Pruebas y revisión

24. Dame una lista de escenarios de fallo (caída de monitor, engine, Central, FO, broker) y, para cada uno, qué debería observar en pantalla. Yo los pruebo y te cuento qué ocurre.
    *Objetivo: plan de pruebas.*
25. Este es el log de un error al arrancar. Antes de darme la solución, ayúdame a interpretarlo: ¿qué línea es la causa y cuáles son consecuencia?
    *Objetivo: aprender a leer trazas.*
26. Quiero defender la práctica. Hazme preguntas difíciles sobre mi código, una a una, y corrígeme si respondo mal.
    *Objetivo: preparación de la defensa.*
