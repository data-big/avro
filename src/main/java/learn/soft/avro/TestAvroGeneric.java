package learn.soft.avro;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import learn.soft.kafka.ProducerPool;

public class TestAvroGeneric {

  private static ProducerPool producerPool = ProducerPool.getInstance();
  public static final String TOPIC = "cache-weibo";
  
	public static void main(String[] args) throws IOException {
		TestAvroGeneric testAvroGeneric = new TestAvroGeneric();
		
		// 1. 将schema从StringPair.avsc文件中加载
		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser
				.parse(testAvroGeneric.getClass().getResourceAsStream("/Student.avsc"));

		// 2. 根据schema创建一个record示例,即我们需要序列化的记录
		GenericRecord genericRecord = new GenericData.Record(schema);
		genericRecord.put("name", "Kami");
		genericRecord.put("favorite_number", 18);
		genericRecord.put("favorite_color", "Red");
		
		//3. 利用avro提供的IO类来做序列化,需要传递 schema对象
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		ByteArrayOutputStream out = new ByteArrayOutputStream();  
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);  
        datumWriter.write(genericRecord, encoder);  
        encoder.flush();  
        out.close();  
  
        byte[] serializedBytes = out.toByteArray();  
        System.out.println("Sending message in bytes : " + serializedBytes);  
        
        Producer<Object, Object> producer = producerPool.getConnection();
        producer.send(new ProducerRecord<Object, Object>(TOPIC, serializedBytes));
        producerPool.returnConnection(producer);
        
      /*  //4. 从文件当中读取,注意需要传递schema对象
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(myFile, datumReader);
        //由于不知道是什么类型，动态加载schema。所以这里用GenericRecord对象
        GenericRecord teacher = null;
        System.out.println("----------------deserializeAvroFromFile-------------------");
        while (dataFileReader.hasNext()) {
            teacher = dataFileReader.next(teacher);
            System.out.println(teacher);
        }*/
	}
}
