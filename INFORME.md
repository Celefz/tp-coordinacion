# TP Coordinación - Informe
A continuación, detallaremos los puntos más importantes de la solución implementada.

## Coordinación entre Sums

Las instancias de `sum` del sistema todas consumen datos de una misma cola de entrada. En un principio, la arquitectura original no contaba con la capacidad de separar los flujos de datos de diversos clientes que estuviesen haciendo request en simultaneo. Para solucionar este problema, se asignó un `clientID` a cada cliente y hizo uso de un mapa para mantener acumulados los resultados por cliente y fruta en cada instancia de `sum`.

Luego, fue necesario implementar un mecanismo de coordinación de modo el sistema pudiese funcionar con varias replicas de `sum`. El problema era que el EOF procesado por el `gateaway` llegaba a un único `sum`, de modo que esa instancia no tenía manera de saber si las demás terminaron de procesar su ingesta, y estas tampoco tenían manera de saber cuando ya no recibirían más records. 

Entonces, como mecanismo de coordinación, se implementó un `controlExchange` que permite a los distintos nodos notificarse entre sí sobre cuando se recibe el EOF y sobre los records que van consumiendo cada uno, hasta verificar que todos hayan sido procesados. El flujo del exchange es el siguiente:

* El `gateaway` recibe el EOF. Entonces, mediante el `messageHandler` lo procesa.
* El `messageHandler` serializa un nuevo mensaje (`EOF_START`) para empezar la coordinación entre sums para manejar en conjutno el EOF. Este nuevo mensaje llega a la cola de input de la cual consumen todos los sums.
* El `sum` que recibe el mensaje de EOF lo propaga por el exchange de control, de modo que el resto de sums se enteren. Este mensaje contiene la cantidad total de records del cliente que tendrán que ser procesados entre todos los sums.
* Cada `sum` va respondiendo con la cantidad de records que procesó (`EOF_PROCESSED`). Esto sigue hasta que se verifique que los records procesados entre todos sumen la cantidad total que debían ser procesados.

## Distribución y Coordinación de Aggregations

En la implementación original, los sums enviaban toda la información procesada a todas las instancias de `aggregation` mediante broadcast. Esto no era del todo correcto pues suponía una cantidad significativa de procesamiento redundante entre los aggregations y hacía practicamente inútil al nodo de `join`, que lo único que hacia que devolver el resultado, sin unificar nada.

Con el fin de solucionar esto, se decidió particionar los resultados entre las distintas instancias de aggregation. Para esto, cada sum cuenta con un middleware asociado a cada aggregation, utilizando un exchange y una routing key diferente para cada instancia. De esta forma, cada sum puede enviar un resultado directamente a la instancia de aggregation que le corresponde.