package com.rahul.consumer.listener;

import com.rahul.consumer.model.PaymentEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class NotificationListener {

    @KafkaListener(topics = "payments", groupId = "notification-service")
    public void handlePayment(ConsumerRecord<String, PaymentEvent> record) {
        PaymentEvent event = record.value();
        System.out.println("📩 SMS to " + record.key() + ": ₹" + event.getAmount()
                + " paid successfully for order " + event.getOrderId());
    }
}