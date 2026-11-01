1. Oye, tengo que leer tramas con formato <STX><DATA><ETX><LRC> desde un socket en Java y no tengo claro con qué clase leerlas. ¿Me explicas la diferencia entre InputStream.read(), BufferedReader y DataInputStream y cuándo uso cada una? No me des el método hecho, que quiero montarlo yo, solo lo que necesito saber para hacerlo.

2. Voy a usar AWS Academy por primera vez. Explícame paso a paso cómo inicio sesión, abro el Learner Lab con Start Lab y sé que ya está listo para entrar a la consola (lo del indicador en verde). Y qué pasa con lo que tengo creado cuando se acaba el tiempo del lab: ¿se pierde o se queda parado?

3. Quiero que Central atienda a varios monitores a la vez. ¿Cómo funciona ServerSocket.accept() metido en un bucle? ¿Y qué opciones tengo para llevar cada conexión (un hilo por conexión, ExecutorService con pool fijo, pool cacheado...)? Dime pros y contras de cada una, pero sin código.

4. ¿Qué hace exactamente Socket.setSoTimeout() y qué excepción salta cuando se pasa el tiempo? Y dame pistas para distinguir en el catch un timeout de que el cliente se haya ido de verdad (read() devolviendo -1, SocketException, EOFException...).

5. ¿Cómo me conecto a mi instancia EC2 una vez creada? Explícame las opciones (EC2 Instance Connect desde el navegador, SSH con el .pem de vockey, Session Manager) y cuándo usaría cada una. Estoy en Windows, así que dime qué cambia con PowerShell o Git Bash, y por qué me puede salir el error de "UNPROTECTED PRIVATE KEY FILE" con la clave y qué comprueba SSH ahí.

6. El monitor tiene que reintentar conectarse a Central cada 3 segundos sin cerrarse. ¿Cómo monto el bucle y las excepciones para que quede bien? Y dime qué tengo que cerrar en cada intento fallido para no dejarme sockets abiertos por ahí.

7. Explícame cómo funciona KafkaConsumer.poll(Duration) y por qué se mete en un bucle. ¿Qué pasa si tardo mucho en procesar los mensajes y no llamo a poll a tiempo (max.poll.interval.ms)? ¿Cómo lo evito sin liarme con hilos por todos lados?

8. Una vez dentro de la instancia, ¿cómo compruebo que Docker está instalado, que el user data ha terminado y que los contenedores de Kafka y Central están levantados? Explícame para qué sirven docker ps, docker ps -a, docker logs -f, docker compose ps y los logs de cloud-init, y cómo sé por los logs que Central ha arrancado bien y que Kafka está listo para recibir conexiones.

9. ¿Cómo se configura un KafkaProducer en Java? Dime qué propiedades son obligatorias (bootstrap.servers, serializadores) y qué cambia con acks y retries. Y el send() es asíncrono, ¿no? ¿Qué devuelve y cómo me entero si ha fallado? Explícame la diferencia entre get() sobre el Future y pasarle un Callback, y cuándo usaría cada uno en Central.

10. Quiero que el hilo consumidor de Central se pare limpio cuando cierro el programa. ¿Para qué sirve wakeup() y cómo va con WakeupException? Y por qué no vale con un boolean volatile, que es lo que se me había ocurrido a mí.

11. Un contenedor me sale en estado "Exited" o reiniciándose en bucle. Dame una lista ordenada de qué miro y en qué orden (logs, código de salida, variables de entorno, puertos ocupados, memoria de la t3.small) para encontrar la causa. Quiero el método, no la solución directa.

12. ConsumerRecord tiene un timestamp(). ¿En qué unidades me lo devuelve y cómo lo comparo con la hora actual para descartar peticiones de más de 10 segundos? ¿Qué problemas me pueden salir si los relojes de las máquinas no van a la par? Pistas, no el código.

13. Cada vez que paro y arranco la instancia me cambia la IP pública y hay que recrearla, porque KAFKA_HOST se fija al arrancar. Explícame qué es una IP elástica, cómo se reserva y se asocia a la instancia, y qué pasa si la reservo y no la tengo asociada (cobro, límites del Learner Lab). ¿Me ahorra recrear la instancia? Y si la uso, ¿qué sitios tengo que cambiar (KAFKA_HOST, CENTRAL_HOST, BROKER_HOST en los equipos del laboratorio)? Ayúdame a hacer la lista.

14. Mi engine tiene que hacer tres cosas a la vez: publicar telemetría cada segundo mientras riega, escuchar los comandos de Kafka y leer el menú por teclado. ¿Qué hilos necesito y qué debería compartir entre ellos? Ayúdame a plantear la estructura, pero sin escribirme el código.

15. Tengo un mapa de estaciones compartido entre varios hilos. ¿Cuándo uso synchronized, ReentrantLock o ConcurrentHashMap? No me queda claro por qué ConcurrentHashMap no me salva si hago "compruebo y luego actúo" en dos pasos; ponme un ejemplo conceptual de cómo fallaría. Y para una bandera "regando", ¿me basta volatile o necesito AtomicBoolean?

16. Quiero una comprobación de punta a punta para ver que todo va en AWS. Desde fuera de la instancia, ¿cómo veo que los puertos 5000, 8080 y 9092 están abiertos de verdad? Explícame qué hacen nc -vz, telnet o Test-NetConnection, y cómo interpreto "refused" frente a "timed out". Y si no llego al panel en http://IP:8080, dime en qué orden compruebo las cosas (instancia en running, IP correcta, security group, firewall, puerto publicado por el contenedor, curl dentro de la propia instancia) y qué descarta cada paso.

17. El engine simula el riego con Thread.sleep() dentro de un bucle. Cuando llega un BLOCK, ¿cómo lo corto bien? Explícame interrupt(), InterruptedException y por qué no es buena idea dejar el catch vacío.

18. Quiero un hilo vigilante que cada segundo mire si algún riego lleva más de 8 segundos sin noticias de su estación. ¿Qué estructura de datos usarías para guardar el instante del último mensaje de cada estación de forma segura entre hilos? Dame ideas y compromisos, no código.

19. Mis estaciones en el laboratorio se conectan a Central pero Kafka da error de conexión o se queda colgado. Me suena que es por lo que Kafka "anuncia" a los clientes (advertised listeners / KAFKA_HOST). Explícame cómo lo compruebo desde un cliente y qué señales en los logs me dicen que el problema es ese y no el security group.

20. Explícame JDBC en Java: DriverManager.getConnection, PreparedStatement, ResultSet y cómo se cierran los recursos. ¿Y por qué tengo que usar PreparedStatement con parámetros en vez de pegar el id de la estación dentro del SQL?

21. ¿Es mejor compartir una sola Connection de SQLite entre todos los hilos o abrir una por operación? Quiero entender los riesgos de cada una en mi caso, que tengo varios HiloWM y el consumidor de Kafka escribiendo a la vez.

22. Explícame cómo funciona el crédito del Learner Lab y qué hay que hacer al terminar las pruebas para no gastarlo: parar vs terminar la instancia, qué pasa con los discos, y si la IP elástica sigue consumiendo cuando la instancia está parada.

23. Cuéntame cómo funciona INSERT ... ON CONFLICT(id) DO UPDATE en SQLite y para qué sirve el excluded. Ponme un ejemplo con otra tabla distinta a la mía para que yo lo adapte al registro de estaciones.

24. Para cerrar un riego tengo que guardar una fila en historico_riegos y poner la estación en DISPONIBLE. Explícame las transacciones en JDBC (setAutoCommit, commit, rollback) y cuándo tiene sentido juntar esas dos operaciones. ¿Qué pasa si falla la segunda?

25. Mi engine es interactivo y con docker compose up no puedo usar el menú. Explícame por qué hay que usar docker compose run --rm y qué significan stdin_open y tty (-i y -t). ¿Qué pasa si no los pongo?

26. El panel lo hago con com.sun.net.httpserver.HttpServer. Explícame createContext, HttpHandler y HttpExchange, y cómo se manda una respuesta bien (cabeceras, sendResponseHeaders con su longitud, cerrar el OutputStream). Sin darme el handler completo.

27. Para mirar la base de datos de Central dentro del contenedor sin tocar nada, ¿cómo uso docker exec? Explícame cómo abrir una shell dentro, cómo ejecutar sqlite3 contra el fichero y cómo copiar el .db a mi PC con docker cp. ¿Qué precauciones tengo para no estropear la base mientras Central escribe?

28. ¿Qué hace el maven-shade-plugin y qué es eso del ManifestResourceTransformer? Y si ejecuto con java -cp app.jar <clase> en vez de java -jar, ¿sigo necesitando el Main-Class? ¿Cuándo sí?

29. En Docker, ¿qué diferencia hay entre ENTRYPOINT y CMD? El operario se lanza con "docker run ... wm-fo <IP> 9092 FO-01" y quiero entender cómo llegan esos argumentos al programa según cómo esté escrito el Dockerfile, y qué pintan las formas exec y shell.

30. Mi SQLite vive en un fichero dentro del contenedor de Central. ¿Qué pasa con los datos si recreo el contenedor? Explícame la diferencia entre un volumen con nombre y un bind mount, y cuál me conviene.

31. Quiero actualizar la imagen de Central en AWS después de un cambio de código. Explícame el ciclo completo: construir la imagen, subirla a Docker Hub, y en la instancia hacer pull y recrear el contenedor. ¿Qué papel tiene la etiqueta latest y por qué a veces el pull no trae lo nuevo?

32. Probando la caída de Central usé docker stop y docker start. Explícame qué diferencia hay entre docker stop, docker kill y docker pause, cuál simula mejor una caída real y qué señales recibe el proceso Java en cada caso. Así sé qué estoy probando de verdad.

33. Arnau mantiene el workflow de GitHub Actions, pero yo quiero saber qué hace cada cosa. Explícame las piezas de un workflow (on, jobs, steps, uses, run, secrets), cómo se cachea Maven y qué hace docker/build-push-action. Dime qué se decide en cada parte, sin dármelo hecho.

34. Somos dos en el repo y yo trabajo en mi rama Alejandro y luego la fusiono en develop. ¿Qué flujo de ramas me recomiendas y qué convenciones de nombres de ramas y de commits son razonables? Explícame también la diferencia entre git merge y git rebase y por qué es peligroso rebasar una rama que ya ha subido mi compañero. Ponme un ejemplo con commits y flechas.

35. No quiero subir la base de datos al repo. Si se me cuela el riego_central.db en un commit, ¿cómo lo dejo de seguir sin borrarlo de mi disco (git rm --cached)? ¿Y si lo quiero quitar también del historial? Dime los riesgos de cada opción y qué debería llevar un .gitignore en un proyecto Maven.

36. He hecho un commit con un cambio que no tocaba. ¿Qué hacen git commit --amend, git restore, git reset --soft, --mixed y --hard? Dime qué parte toca cada uno (directorio de trabajo, índice, HEAD) para elegir bien y no perder trabajo. Estoy en Windows con Git Bash, por si cambia algo.

37. Mi EstacionDAO se está llenando de cosas: estados, volumen, histórico, riegos abandonados. Explícame bien el patrón DAO, qué le toca y qué no, y dame criterios para saber si debería separar parte de esa lógica en otra clase. No me lo refactorices, que me gusta respetar lo que ya tengo.

38. La tabla de estados depende de si el monitor y el engine están vivos, más el FUERA_DE_SERVICIO del panel. ¿Cómo modelo una máquina de estados en Java (enum con transiciones, switch, mapa de transiciones) para no llenar todo de if anidados? ¿Cómo decido qué transiciones no son válidas?

39. Si llega dos veces un AVERIA#WS-01, al operario solo se le debe avisar una vez. Explícame qué es la idempotencia en mensajería y qué estrategias hay para que repetir un mensaje no repita sus efectos. Quiero entender las opciones antes de elegir una.

40. Quiero hacer tests con JUnit 5 de calcularLRC y de la validación de tramas. ¿Qué casos límite debería probar (datos vacíos, caracteres no ASCII, ETX dentro de DATA, LRC corrupto)? Y explícame qué es un test parametrizado y cuándo meterlo. No me escribas los tests.

41. Quiero depurar sin escribir código: ¿cómo mando una trama a mano al puerto 5000 con nc o telnet respetando STX, ETX y LRC? ¿Y cómo leo un topic con kafka-console-consumer, incluso con un grupo concreto? Dame los comandos y qué hace cada opción.

42. Explícame las garantías at-most-once, at-least-once y exactly-once en Kafka. ¿Cuál tiene de verdad mi flujo de riego con auto-commit y con commit manual? Dame un escenario de fallo para cada caso y yo razono qué pasaría con una petición de riego.

43. Quiero prepararme para defender la práctica siendo honesto con el uso de la IA. ¿Qué preguntas suele hacer un profesor para comprobar que entiendes tu propio código? ¿Y cómo me preparo para explicar una clase, por ejemplo HiloWM, línea por línea?

44. ¿Qué pros y contras tiene que solo Central acceda a la base de datos? Explícame lo de cuello de botella y punto único de fallo aplicado a mi arquitectura, y qué alternativas habría (réplicas, base de datos por servicio) para poder comentarlas en la defensa aunque no las hayamos hecho.

45. Quiero una guía de comprobación para el día de la defensa en AWS: qué debo tener listo antes (lab iniciado, instancia en marcha, IP anotada, imágenes públicas en Docker Hub, estaciones y operarios preparados), en qué orden arranco todo y qué compruebo en cada paso. Hazme preguntas para que yo construya la checklist, no me la des hecha.