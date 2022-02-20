package edu.uci.ics.tippers.listener;

import java.sql.Time;
import java.sql.Date;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import edu.uci.ics.tippers.config.MessageSerializer;
import edu.uci.ics.tippers.dbms.MallData;
import edu.uci.ics.tippers.model.middleware.LeTime;
import edu.uci.ics.tippers.model.middleware.Message;
import edu.uci.ics.tippers.model.middleware.mget_obj;
import edu.uci.ics.tippers.producer.ProduceResults;
import edu.uci.ics.tippers.execution.MiddleWare.ops;

@Service
public class KafkaConsumer {
    private static final String topic = "results";
    @Autowired
    KafkaTemplate<String, Message> kafkaTemplate;
    @KafkaListener(topics = "cloud", groupId = "group_id")
    public void consume(String message) {
        System.out.println("Consume message " + message);
    }

    @KafkaListener(topics = "mget_obj", groupId = "group_json", containerFactory = "mget_objKakfaListenerFactory")
    public void consumemget_obj(mget_obj qm) {
        System.out.println("Consume json " + qm);
    }

    @KafkaListener(topics = "query", groupId = "group_json", containerFactory = "mget_objKakfaListenerFactory")
    public void executeQuery(mget_obj qm) {
        String querier = qm.getId();
        String prop = qm.getProp();
        String info = qm.getInfo();
        System.out.println("querier: " + querier + " prop: "+ prop + " info: " + info);
        ops op = new ops();
        MallData[] res = op.get(querier, prop, info);
        Message msg;
        if (res == null) {
            msg = new Message("Fail, data not found", res);
        } else {
            msg = new Message("Succ", res);
        }
        ProduceResults pr = new ProduceResults();
        pr.sendResults(msg);
    }

    @KafkaListener(topics="letime", groupId = "group_json", containerFactory = "leTimeKafkaListenerFactory")
    public void checkTime(LeTime lt) {
        System.out.println(lt.getTime());
        MallData[] md = new MallData[1];
        md[0] = new MallData("id", "shop_name", new Date(System.currentTimeMillis()), new Time(System.currentTimeMillis()), "user_interest", 2);
        Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", MessageSerializer.class);

		KafkaProducer<String, Message> producer = new KafkaProducer<>(props);
        producer.send(new ProducerRecord<String, Message>(topic, new Message("Succ", md)));
        producer.close();
    }
}
