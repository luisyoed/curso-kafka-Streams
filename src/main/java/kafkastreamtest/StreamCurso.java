package kafkastreamtest;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;



public class StreamCurso {

	public static void main(String[] args) {
		
		Properties props = new Properties();
	     props.put(StreamsConfig.APPLICATION_ID_CONFIG,"cursokafkaa");
	     props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
	     props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
	     props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
	     
	     final StreamsBuilder builder = new StreamsBuilder();
	     final KStream<String, String> source = builder.stream("curso-kafka-test-5-in");
	     
	     source.flatMapValues(v -> {
	    	 ArrayList<String> r = new ArrayList<String>();
	    	 r.add("word: '" + v + "', length: " + v.length() + ", words: " + v.split(" ").length);
	    	 System.out.println("word: '" + v + "', length: " + v.length() + ", words: " + v.split(" ").length);
	    	 return r;
	     }).to("curso-kafka-test-5-out");
	     
	     System.out.println("Esperando conexiones.....");
	     final KafkaStreams streams = new KafkaStreams(builder.build(), props);
	     streams.start();

	}

}
