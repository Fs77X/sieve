package edu.uci.ics.tippers.listener;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import edu.uci.ics.tippers.model.middleware.LeTime;
import edu.uci.ics.tippers.model.middleware.mget_obj;

@Service
public class KafkaConsumer {
    @KafkaListener(topics = "cloud", groupId = "group_id")
    public void consume(String message) {
        System.out.println("Consume message " + message);
    }

    @KafkaListener(topics = "mget_obj", groupId = "group_json", containerFactory = "mget_objKakfaListenerFactory")
    public void consumemget_obj(mget_obj qm) {
        System.out.println("Consume json " + qm);
    }
    @KafkaListener(topics="letime", groupId = "group_json", containerFactory = "leTimeKafkaListenerFactory")
    public void checkTime(LeTime lt) {
        System.out.println(lt.getTime());
    }
}
