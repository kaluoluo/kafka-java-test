package product;

import jdk.nashorn.internal.objects.annotations.Function;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

public class Product {
    public static void main(String[] args) throws Exception{
        Properties properties = new Properties();
        //指定kafka服务器地址 如果是集群可以指定多个  但是就算只指定一个他也会去集群环境下寻找其他的 节点地址
        properties.setProperty("bootstrap.servers","118.190.36.109:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(properties);
        ProducerRecord<String,String> producerRecord = new ProducerRecord<String, String>("test_topic","restKey","hello");
        Future<RecordMetadata> recordMetadataFuture = kafkaProducer.send(producerRecord);
        RecordMetadata metadata = recordMetadataFuture.get();
        System.out.println(metadata.partition()+"------");
        kafkaProducer.flush();
        kafkaProducer.close();


    }
}
