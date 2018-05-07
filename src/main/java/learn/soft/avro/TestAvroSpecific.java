package learn.soft.avro;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

public class TestAvroSpecific {

	public static void main(String[] args) throws IOException {

		// 方法1：使用new方法
		Student s1 = new Student("Kami", 10, "red");

		// 方法2：使用set方法
		Student s2 = new Student();
		s2.setName("David");
		s2.setFavoriteNumber(20);
		s2.setFavoriteColor("Green");

		// 方法3：使用build 方法
		Student s3 = Student.newBuilder().setName("Lucy").setFavoriteNumber(15).setFavoriteColor("yellow").build();

		List<Student> students = new ArrayList<>();
		students.add(s1);
		students.add(s2);
		students.add(s3);

		serializeAvroToFile(students, "/home/gy/works/avro/file/myFile.txt");
		deserializeAvroFromFile("/home/gy/works/avro/file/myFile.txt");
	}

	// 使用avro协议序列化对象到文件中
	public static void serializeAvroToFile(List<Student> students, String fileName) throws IOException {
		DatumWriter<Student> datumWrite = new SpecificDatumWriter<>(Student.class);
		DataFileWriter<Student> dataFileWriter = new DataFileWriter<>(datumWrite);
		dataFileWriter.create(students.get(0).getSchema(), new File(fileName));
		for (Student student : students) {
			dataFileWriter.append(student);
		}
		dataFileWriter.close();
	}

	// 使用avro协议反序列化对象到内存并且打印
	public static void deserializeAvroFromFile(String fileName) throws IOException {
		File file = new File(fileName);
		DatumReader<Student> datumReader = new SpecificDatumReader<>(Student.class);
		DataFileReader<Student> dataFileReader = new DataFileReader<>(file, datumReader);
		Student student = null;
		System.out.println("----------------deserializeAvroFromFile-------------------");
		while (dataFileReader.hasNext()) {
			student = dataFileReader.next(student);
			System.out.println(student);
		}
	}
}
