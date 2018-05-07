package learn.soft.avro;

import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;

import io.confluent.kafka.serializers.KafkaAvroSerializer;



public class ProducerAvro {

  private static KafkaProducer<Object, Object> producer;

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        KafkaAvroSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
       KafkaAvroSerializer.class);
    props.put("schema.registry.url", "http://localhost:8081");
    producer = new KafkaProducer<Object, Object>(props);

    String userSchema = "{\"type\":\"record\"," + "\"name\":\"myrecord\","
        + "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(userSchema);
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("f1", "value1");

    ProducerRecord<Object, Object>  record = new ProducerRecord<Object, Object>("cache-weibo", avroRecord);
    try {
      producer.send(record);
    } catch (SerializationException e) {
      e.printStackTrace();
    }
  }
}
