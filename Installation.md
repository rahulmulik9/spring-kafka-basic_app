1. Download Kafka — `kafka_2.13-4.3.1.tgz` from `https://kafka.apache.org/downloads`, extract using 7-Zip to `C:\kafka_2.13-4.3.1`
2. Fix the Windows `wmic` issue — open `bin\windows\kafka-server-start.bat`, replace:

```
   IF ["%KAFKA_HEAP_OPTS%"] EQU [""] (
       rem detect OS architecture
       wmic os get osarchitecture | find /i "32-bit" >nul 2>&1
       IF NOT ERRORLEVEL 1 (
           rem 32-bit OS
           set KAFKA_HEAP_OPTS=-Xmx512M -Xms512M
       ) ELSE (
           rem 64-bit OS
           set KAFKA_HEAP_OPTS=-Xmx1G -Xms1G
       )
   )
```

with

```
   IF ["%KAFKA_HEAP_OPTS%"] EQU [""] (
       set KAFKA_HEAP_OPTS=-Xmx1G -Xms1G
   )
```

1. Generate cluster ID (run once only):

```java
Open terminal in kafka folder
  cd C:\kafka_2.13-4.3.1
        
   .\bin\windows\kafka-storage.bat random-uuid
```

1. Format storage (run once only, using the UUID from step 3):

```
   .\bin\windows\kafka-storage.bat format --standalone -t <uuid> -c .\config\server.properties
```

1. Start Kafka (run this every time you want Kafka running):

```
   .\bin\windows\kafka-server-start.bat .\config\server.properties
```

1. (Optional, one-time test) Create a topic, then test producer/consumer:

```
   .\bin\windows\kafka-topics.bat --create --topic payments --bootstrap-server localhost:9092
   .\bin\windows\kafka-console-producer.bat --topic payments --bootstrap-server localhost:9092
   .\bin\windows\kafka-console-consumer.bat --topic payments --from-beginning --bootstrap-server localhost:9092
```

next time only run this

```
   .\bin\windows\kafka-server-start.bat .\config\server.properties
```