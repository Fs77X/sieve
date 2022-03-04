package edu.uci.ics.tippers.listener;

import java.sql.Time;
import java.io.IOException;
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
import edu.uci.ics.tippers.model.middleware.LogMessage;
import edu.uci.ics.tippers.model.middleware.Message;
import edu.uci.ics.tippers.model.middleware.MetaData;
import edu.uci.ics.tippers.model.middleware.QueryKafka;
import edu.uci.ics.tippers.producer.LogResults;
// import edu.uci.ics.tippers.model.middleware.mget_obj;
import edu.uci.ics.tippers.producer.ProduceResults;
import edu.uci.ics.tippers.response.CloudResponse;
import edu.uci.ics.tippers.execution.MiddleWare.ops;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
@Service
public class KafkaConsumer {
    private static final String topic = "results";

    private void madd_obj(MallData mallData, MetaData metaData) {
        ops op = new ops();
    }

    private void mget_objUSR(String querier, String prop, String info) {
        ops op = new ops();
        MallData[] res = op.getpersonalData(info);
        Message msg;
        if (res == null) {
            msg = new Message("Fail, data not found", res, null, "", "");
        } else {
            msg = new Message("Succ", res, null, "", "");
        }
        ProduceResults pr = new ProduceResults();
        pr.sendResults(msg);
    }

    private void mget_obj(String querier, String prop, String info, String qid, QueryKafka qk) {
        ops op = new ops();
        System.out.println(querier + " " + qid);
        MallData[] res = op.get(querier, prop, info);
        Message msg;
        if (res == null) {
            msg = new Message("Fail, data not found", res, null, qid, querier);
        } else {
            msg = new Message("Succ", res, null, qid, querier);
        }
        CloudResponse cr = new CloudResponse();
        cr.sendResponse(msg);
        try {
            ObjectMapper mapper = new ObjectMapper();
            String operation = mapper.writeValueAsString(qk);
            LogMessage lm = new LogMessage(querier, operation);
            LogResults lr = new LogResults();
            lr.sendResults(lm);
        } catch (JsonParseException e) { e.printStackTrace();}
        catch (JsonMappingException e) { e.printStackTrace(); }
        catch (IOException e) { e.printStackTrace(); }
        
    }

    private void mget_entry(String querier, String prop, String info, String qid, QueryKafka qk) {
        ops op = new ops();
        MallData[] res = op.getpersonalEntry(info);
        Message msg;
        if (res == null) {
            msg = new Message("Fail, data not found", null, null, qid, querier);
        } else {
            msg = new Message("Succ", res, null, qid, querier);
        }
        CloudResponse cr = new CloudResponse();
        cr.sendResponse(msg);
        try {
            ObjectMapper mapper = new ObjectMapper();
            String operation = mapper.writeValueAsString(qk);
            LogMessage lm = new LogMessage(querier, operation);
            LogResults lr = new LogResults();
            lr.sendResults(lm);
        } catch (JsonParseException e) { e.printStackTrace();}
        catch (JsonMappingException e) { e.printStackTrace(); }
        catch (IOException e) { e.printStackTrace(); }

    }

    private void mget_metaEntry(String querier, String prop, String info) {
        ops op = new ops();
    }

    private void mdelete_obj(String key) {
        ops op = new ops();
        int status = op.removeUserEntry(key);
        ProduceResults pr = new ProduceResults();
        Message msg;
        if (status != 0) {
            msg = new Message("Fail to update", null, null, "", ""); 
        }
        else {
            msg = new Message("Succ", null, null, "", ""); 
        }
        pr.sendResults(msg);

    }

    private void mmodify_obj(String updateKey, String prop, String info) {
        ops op = new ops();
        int status = op.updateEntry(updateKey, prop, info);
        ProduceResults pr = new ProduceResults();
        Message msg;
        if (status != 0) {
            msg = new Message("Fail to update", null, null, "", ""); 
        }
        else {
            msg = new Message("Succ", null, null, "", ""); 
        }
        pr.sendResults(msg);
    }

    private void mmodify_metaobj(String updateKey, String prop, String info) {
        ops op = new ops();
        int status = op.updateMetaEntry(updateKey, prop, info);
        System.out.println(status);
        ProduceResults pr = new ProduceResults();
        Message msg;
        if (status != 0) {
            msg = new Message("Fail to update", null, null, "", ""); 
        }
        else {
            msg = new Message("Succ", null, null, "", ""); 
        }
        pr.sendResults(msg);
    }

    @Autowired
    KafkaTemplate<String, Message> kafkaTemplate;

    @KafkaListener(topics = "cloud", groupId = "group_id")
    public void consume(String message) {
        System.out.println("Consume message " + message);
    }

    @KafkaListener(topics = "mget_obj", groupId = "group_json", containerFactory = "queryKafkaKakfaListenerFactory")
    public void consumemget_obj(QueryKafka qm) {
        System.out.println("Consume json " + qm);
    }

    @KafkaListener(topics = "query", groupId = "group_json", containerFactory = "queryKafkaKakfaListenerFactory")
    public void executeQuery(QueryKafka qm) {
        String querier = qm.getId();
        String prop = qm.getProp();
        String info = qm.getInfo();
        String query = qm.getQuery();
        String updateKey = qm.getUpdateKey();
        MetaData metaData = qm.getMetaData();
        MallData mallData = qm.getMallData();
        String qid = qm.getQid();
        System.out.println("querier: " + querier + " prop: " + prop + " info: " + info + " query: " + query);
        switch (query) {
            case "mget_obj":
                mget_obj(querier, prop, info, qid, qm);
                break;
            case "mget_entry":
                mget_entry(querier, prop, info, qid, qm);
                break;
            case "mget_objUSR":
                mget_objUSR(querier, prop, info);
                break;
            case "mmodify_obj":
                mmodify_obj(updateKey, prop, info);
                break;
            case "mmodify_metaobj":
                mmodify_metaobj(updateKey, prop, info);
                break;
            case "mget_metaEntry":
                break;
            case "mdelete_obj":
                mdelete_obj(updateKey);
                break;
            case "madd_obj":
                madd_obj(mallData, metaData);
                break;
            default:
                System.out.println("uhoh");
        }
    }

    @KafkaListener(topics = "letime", groupId = "group_json", containerFactory = "leTimeKafkaListenerFactory")
    public void checkTime(LeTime lt) {
        System.out.println(lt.getTime());
        MallData[] md = new MallData[1];
        md[0] = new MallData("id", "shop_name", new Date(System.currentTimeMillis()),
                new Time(System.currentTimeMillis()), "user_interest", 2);
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
        producer.send(new ProducerRecord<String, Message>(topic, new Message("Succ", md, null, "", "")));
        producer.close();
    }
}
