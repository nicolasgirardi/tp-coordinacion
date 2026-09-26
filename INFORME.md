# Informe
En este informe se van a detallar los problemas que fueron apareciendo a lo largo del desarrollo del trabajo práctico y cómo se solucionaron

## Múltiples Clientes

Para empezar a poder manejar múltiples clientes se debió resolver el problema de separar la información de cada cliente para evitar cruces.

Esto se solucionó asignando un ID único a cada cliente. Como el punto de entrada al sistema es el gateway, este se debía generar aquí. Aprovechando
que cada vez que se conecta un cliente se crea una instancia de MessageHandler, el cual es único para el cliente y vive mientras el cliente esté conectado,
este será quien tenga dicho ID, la cual es generada de forma secuencial.

Cada vez que se envíe un mensaje asociado al cliente, este deberá contener el ID del mismo para así evitar cruces de datos.

Para esto se modificaron las funciones de serealización y deseralizacion de inner para que incluyeran el ID.

También se tuvo que modificar a sum y aggregation para manejar este ID en sus handlers y que al momento de guardar datos en sus diccionarios también usen
el ID como clave.

## Múltiples Sum

El problema que se presenta con múltiples sum es que sólo uno de estos va a recibir el mensaje de eof por parte del gateway. 

Para coordinar la entrega de datos hacia el aggregation se decidió por tener un exchange exclusivo entre los sum en el cual todos
pueden publicar y todos pueden escuchar. Cuando a uno de los sum le llega un mensaje de eof, este lo replica en el exchange y todos los sum, 
incluido este, lo van a recibir.

Cuando a un sum le llega el eof mediante dicho exchange, envía los datos de la misma forma que lo hacía antes.

Es posible que un sum esté procesando un mensaje de un cliente en el mismo momento que le llega mediante el exchange un mensaje de eof relacionado
con el mismo cliente. Para eso se protegieron los handlers del mensaje desde el gateway y desde los demás sum con el mismo mutex. De esta forma, hasta que sum
no termine de procesar el mensaje con el que esté trabajando, no va a procesar el eof.

También se modificó el aggregation para que ahora espere una cantidad de mensajes eof igual a la cantidad de instancias de sum que hay

## Múltiples Aggregation

El problema que se genera con múltiples aggregation es que se deben distribuir los datos de cada cliente de tal forma que todos los datos de un
tipo de fruta vayan al mismo aggregation.

Esto se consigue haciendo que cada uno de los aggregation escuchen a 1 solo tópico del exchange que comparten con los sum. Luego del lado del sum
se le asigna un hash a cada registro basándose en nombre de la fruta y el ID del cliente, el cual luego se mapea a uno de los tópicos posibles.

De esta forma nos garantizamos que todos los datos del mismo cliente y la misma fruta, como banana 1, vayan al mismo aggregation. 

Por último se modifica el join para que junte los tops parciales de los aggregation y le envíe al gateway el top final. 

## Escalabilidad

El sistema escala respecto a los clientes y el volumen de datos gracias a que se pueden tener un número arbitrio de instancias de sum
y aggregation necesarios para manejar un número arbitrario de clientes o cantidad arbitraria de datos.