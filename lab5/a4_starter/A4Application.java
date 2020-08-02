import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.util.Arrays;
import java.util.Properties;


public class A4Application {

    public static void main(String[] args) throws Exception {
		// do not modify the structure of the command line
		String bootstrapServers = args[0];
		String appName = args[1];
		String studentTopic = args[2];
		String classroomTopic = args[3];
		String outputTopic = args[4];
		String stateStoreDir = args[5];

		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.STATE_DIR_CONFIG, stateStoreDir);

		// add code here if you need any additional configuration options
		StreamsBuilder builder = new StreamsBuilder();
		// add code here
		KStream<String,String> studentInfo = builder.stream(studentTopic);
		KStream<String,String> classroomCapacity = builder.stream(classroomTopic);

		//reduce studentInfo
		KTable<String,String> student_classroom = studentInfo.groupBy((key,value)-> key).reduce((aggValue, newValue) -> newValue);
		//reduce classroomCapacity
		KTable<String,String> classroom_capacity = classroomCapacity.groupBy((key,value) -> key).reduce((aggValue, newValue) -> newValue);

		KTable<String,Long> classromm_curcap = student_classroom.groupBy((key,value) -> KeyValue.pair(value,key)).count();
		//join
		KTable<String,String> classroom_curcap_cap = classromm_curcap.join(classroom_capacity,
				(leftValue,rightValue) -> leftValue.toString()+","+rightValue.toString()
				);

		classroom_curcap_cap.toStream().foreach((key,value) -> System.out.println(key + " : " + value));
		//compare and output
		KTable<String,String> output = classroom_curcap_cap.groupBy((key,value)-> key).aggregate(
				()->"",
				(aggKey, newValue, oldValue)->{
					String status=null;
					Int currentCapacity = (Int)newValue.toString()[0];
					Int totalCapacity = (Int)newValue.toString()[1];
					if(currentCapacity>totalCapacity){
						status=(String)(currentCapacity-totalCapacity);
					}else{
						status="OK";
					}
					return KeyValue.pair(aggKey,status);
				}
		);

		output.toStream().to(outputTopic,Produced.with(Serdes.String(),Serdes.String()));

		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		// this line initiates processing
		streams.start();

		// shutdown hook for Ctrl+C
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
