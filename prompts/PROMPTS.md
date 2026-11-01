# Uso de IA — WaterManagement

**Herramienta:** Claude (Anthropic), en conversación, sin acceso al repositorio.
**Matias Argüello:** Monitor, Engine, Operario, Docker y pruebas.

La usé para planificar, para que me explicara conceptos (sockets, Kafka, Docker) y para obtener el codigo por pasos pequeños, pidiendo la explicación de cada bloque. Cada paso lo escribí en mi rama, lo probé antes del commit y si fallaba, volvía con el error para ajustarlo.

| Fecha | Prompt (resumen) | Qué hice con la respuesta |

| 14/10 | Revisar el código inicial y repartir tareas; clase de protocolo con tramas STX/ETX/LRC | Plan de fases y base de `Protocolo.java`, adaptada entre los dos |
| 15/10 | Monitor: registro en la central y servidor del Engine con SALUD cada segundo, explicado bloque a bloque | Implementado y probado contra la central local |
| 15/10 | Engine: conexión al Monitor y avería simulada con una tecla | Implementado y probado |
| 20/10 | "Si cierro la central se caen el Monitor y el Engine" | Reconexiones con timeouts y sincronización del estado de avería |
| 20/10 | Engine con Kafka: órdenes filtradas, riego con telemetría y menú; "la orden llega pero no riega" | Implementado; el menú solo aparece cuando Kafka asigna particiones |
| 20/10 | Operario: menú, notificaciones y modo fichero según el PDF | Implementado. Decidí no validar en el operario lo que ya valida la central |
| 21/10 | Docker y scripts para no abrir 6 terminales | Dockerfiles, `docker-compose.yml`, `estacion.sh` y `operario.sh` |
| 21/10 | Automatizar escenarios de prueba; mensajes que se perdían en Kafka | Script de pruebas. Rechacé cambiar a `assign()` por alterar la arquitectura del README; cierre ordenado del consumidor y `session.timeout.ms` a 10 s |
| 28/10 | Explicación de topics, particiones, grupos y escalado por provincia | Estudio para la defensa, sin cambios de código |
| 30/10 | Revisar el proyecto contra la guía de corrección | Lista de pendientes repartida entre los dos |


---

**Parte Yousra:** WM_Central (sockets, lógica de riegos, base de datos, Kafka y panel web) y despliegue en Railway.
**Herramienta:** Claude Code.

La usé para planificar, entender los conceptos y el código, implementar, depurar y desplegar la central. Le pedía que me explicara cada parte para entender lo que hacía. Yo tomaba las decisiones, probaba el funcionamiento y le comunicaba los errores para corregirlos. El panel web lo generó la IA, porque no tenía experiencia con HTML.

| Fecha | Prompt (resumen) | Qué hice con la respuesta |
|---|---|---|
| 14/10 | Planificar mi parte y organizar el trabajo con Matías | Acordamos el reparto de tareas, respetando la estructura exigida |
| 14–16/10 | Servidor de sockets de la central, protocolo STX/ETX/LRC y conexión con Kafka | Pedí la explicación de cada parte, lo probé y le pasé los errores para corregirlos |
| 15/10 | Guardar y cargar las estaciones en SQLite | Comprobé que las estaciones se recuperan al reiniciar la central |
| 18/10 | Lógica de riego y comunicación entre operario, central y estaciones | Revisé las explicaciones y probé el ciclo completo |
| 18–26/10 | Generar el panel web y mejorar su diseño y funciones | La IA generó la página; probé los estados y las órdenes desde el navegador |
| 19–26/10 | Desplegar la central y Kafka en Railway | Seguí los pasos, configuré el despliegue y lo probé en el aula |
| 28–30/10 | Escalabilidad, resiliencia y cambios pedidos por el profesor | Decidí qué cambiar, lo probé y comprobé que funcionara |