# Kafka with Spring Boot — Payment Flow Example

A two-service example: a **Producer** service that publishes payment events, and a **Consumer** service that reads them and sends a notification.

---

## Required Dependencies (both services)

- **spring-boot-starter-webmvc** — REST API support (`@RestController`, etc.)
- **spring-boot-starter-kafka** — Kafka support (`KafkaTemplate`, `@KafkaListener`)
- **jackson-databind** — converts Java objects to/from JSON
- **lombok** — removes boilerplate (getters/setters/constructors)

---

## Shared Model (used in both services, but as separate copies in each project)

```java
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PaymentEvent {
    private String orderId;
    private double amount;
    private String status;
}
```

- `@Data` — Lombok generates getters, setters, `toString()`, `equals()`, `hashCode()`
- `@NoArgsConstructor` — generates an empty constructor (required for JSON deserialization)
- `@AllArgsConstructor` — generates a constructor with all fields

---

# PRODUCER SERVICE

## Controller

```java
@RestController
public class PaymentController {

    private final KafkaTemplate<String, PaymentEvent> kafkaTemplate;

    public PaymentController(KafkaTemplate<String, PaymentEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/pay")
    public String pay() {
        String orderId = "asdf34ass";
        PaymentEvent event = new PaymentEvent(orderId, 500, "SUCCESS");
        kafkaTemplate.send("payments", "Rahul", event);
        return "Payment event sent for order " + orderId;
    }
}
```

### Explaining `kafkaTemplate.send("payments", "Rahul", event)`

`KafkaTemplate` is Spring's helper class for sending messages to Kafka — it wraps the raw Kafka producer client so you don't have to deal with low-level details.

`.send(topic, key, value)` takes **three arguments**:

1. **`"payments"`** — the **topic** name. This is *where* the message is written. Both producer and consumer must agree on this exact name — it's the shared contract between them.

2. **`"Rahul"`** — the **key**. Every Kafka message has a key, used for two things:
    - It determines **which partition** the message goes to (Kafka hashes the key to decide). All messages with the same key always land in the same partition, which guarantees they stay in order relative to each other.
    - It's available to the consumer too (`record.key()`), so it can be used as extra context — here, it's being (mis)used as "who to notify," which works for this simple example but in a real system the key would usually be something like the `orderId` or `userId` for proper partitioning.

3. **`event`** — the **value**. This is the actual payload — your `PaymentEvent` object. Spring's `JsonSerializer` (configured in properties) automatically converts this Java object into JSON bytes before sending.

`KafkaTemplate` is **generic**: `KafkaTemplate<String, PaymentEvent>` means "keys are Strings, values are `PaymentEvent` objects." This must match the serializers configured in `application.properties`.

---

## Producer's `application.properties`

```properties
spring.application.name=producer
server.port=8081
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer
```

| Property | Meaning |
|---|---|
| `spring.application.name=producer` | Just a label for this app — shows up in logs (`[producer]`). No effect on Kafka behavior. |
| `server.port=8081` | The port this Spring Boot app's own REST API runs on (where `/pay` lives). Regular Spring Boot config, unrelated to Kafka. |
| `spring.kafka.bootstrap-servers=localhost:9092` | **The important one.** Tells the app where the Kafka broker is running, so it can connect. `9092` is Kafka's default port. |
| `spring.kafka.producer.key-serializer` | Kafka only stores raw bytes — it doesn't know what a "String" or "Object" is. This tells Kafka how to convert the **key** (`"Rahul"`, a plain string) into bytes before sending. `StringSerializer` = "the key is text." |
| `spring.kafka.producer.value-serializer` | Same idea, but for the **value** (the `PaymentEvent` object). Since it's a Java object, not a string, `JsonSerializer` converts it to JSON text before sending. |

---

# CONSUMER SERVICE

## Model (own copy, own package)

```java
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PaymentEvent {
    private String orderId;
    private double amount;
    private String status;
}
```

> Important: this is a **separate class**, living in the consumer's own package (e.g. `com.rahul.consumer.model.PaymentEvent`) — it is not literally shared code between the two services, just the same shape/structure copied into both projects.

## Listener

```java
@Component
public class NotificationListener {

    @KafkaListener(topics = "payments", groupId = "notification-service")
    public void handlePayment(ConsumerRecord<String, PaymentEvent> record) {
        PaymentEvent event = record.value();
        System.out.println("📩 SMS to " + record.key() + ": ₹" + event.getAmount()
                + " paid successfully for order " + event.getOrderId());
    }
}
```

### Explaining the pieces


- **`@KafkaListener(topics = "payments", groupId = "notification-service")`** — this is what makes the method actually *listen*. It tells Spring: "run this method automatically whenever a new message arrives on the `payments` topic."
    - `topics = "payments"` — must match the topic name the producer sends to.
    - `groupId = "notification-service"` — the **consumer group** this listener belongs to (explained below in the properties section — it's arbitrary text you define).

- **`ConsumerRecord<String, PaymentEvent> record`** — the full incoming message wrapper. It contains both the key and value, plus metadata (partition, offset, timestamp) if you ever need it.
    - `record.key()` — retrieves `"Rahul"` (the key the producer sent).
    - `record.value()` — retrieves the deserialized `PaymentEvent` object.

- **Why this method signature works automatically:** Spring Kafka handles polling, deserialization, and invoking this method for you — you never manually call `.poll()` like you would with the raw Kafka client. You just write what should happen *when* a message arrives.

---

## Consumer's `application.properties`

```properties
spring.application.name=consumer
server.port=8082
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.consumer.group-id=notification-service
spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer
spring.kafka.consumer.properties.spring.json.trusted.packages=*
```

| Property | Meaning |
|---|---|
| `spring.kafka.consumer.group-id=notification-service` | Identifies which **consumer group** this app belongs to. Arbitrary text you choose. Kafka uses this to track *how far this group has read* on a topic. Two consumer instances with the **same** group-id are treated as teammates (work is split between them). Two with **different** group-ids are treated as independent — each gets its own full copy of every message. This has nothing to do with the producer — it's purely a consumer-side concept. |
| `spring.kafka.consumer.auto-offset-reset=earliest` | Controls where a consumer group starts reading from **the first time it connects** (no prior progress recorded). `earliest` = read from the very beginning of the topic, including old messages already there. The alternative, `latest`, means "only read new messages from now on." |
| `key-deserializer` / `value-deserializer` | The reverse of the producer's serializers — converts raw bytes back into a String (key) and Java object (value). |
| `spring.kafka.consumer.properties.spring.json.trusted.packages=*` | A security setting for `JsonDeserializer`. By default, Spring refuses to deserialize into arbitrary Java classes (to prevent malicious payloads instantiating dangerous classes). `*` means "trust everything." In production, this should be scoped to your own package instead, e.g. `com.rahul.consumer.model`. |

### Known gotcha: cross-service class name mismatch

By default, `JsonSerializer` embeds the producer's fully-qualified class name (e.g. `com.rahul.producer.model.PaymentEvent`) into the message. If the consumer's class lives in a different package (e.g. `com.rahul.consumer.model.PaymentEvent`), deserialization fails with `ClassNotFoundException`, and the consumer gets stuck retrying the same message forever.

**Fix** — tell the consumer to ignore the producer's class name and always deserialize into its own local class:

```properties
spring.kafka.consumer.properties.spring.json.use.type.headers=false
spring.kafka.consumer.properties.spring.json.value.default.type=com.rahul.consumer.model.PaymentEvent
```

---

## The Core Pattern to Remember

- **Producer config** always has **serializers** (Java object → bytes)
- **Consumer config** always has **deserializers** (bytes → Java object)
- **Topic name** is the shared contract both sides must agree on
- **Group ID** belongs entirely to the consumer side and controls whether multiple consumer instances share work or duplicate it