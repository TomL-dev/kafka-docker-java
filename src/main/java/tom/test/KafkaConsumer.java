package tom.test;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

public class KafkaConsumer {


	public static void main(String[] args) throws Exception {
		Consumer<Long, String> consumer = createConsumer();
		runConsumer(consumer, 10);


//		Map<String, List<PartitionInfo>> topics = consumer.listTopics();
//
//		List<PartitionInfo> partition = topics.get(KafkaSettings.TOPIC);
//
//		PartitionInfo info = partition.get(0);
//		TopicPartition topicPartition = new TopicPartition(KafkaSettings.TOPIC, info.partition());
//		System.out.println(topicPartition);
//		final Collection<TopicPartition> c = Collections.singletonList(topicPartition);
//		System.out.println(c);
//
//		System.out.println("seek to beginning");
//		consumer.assign(c);
//		consumer.seekToBeginning(c);
//
//		consumer.close();
//
//		consumer = createConsumer();
//		consumer.subscribe(Collections.singletonList(KafkaSettings.TOPIC));
//		for (int i = 0; i < 1000000; i++){
//			ConsumerRecords<Long, String> records = consumer.poll(100);
//			records.forEach(record -> System.out.printf("Consumer Record:(%d, %s)\n", record.key(), record.value()));
//		}
//		consumer.unsubscribe();
//		consumer.close();

	}

	private static Consumer<Long, String> createConsumer() {
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaSettings.SERVER);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		// Create the consumer using props.
		final Consumer<Long, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
		// Subscribe to the topic.
		//consumer.subscribe(Collections.singletonList(KafkaSettings.TOPIC));
//		consumer.assign(/*Collections.singletonList(KafkaSettings.TOPIC)*/);

		return consumer;
	}

	static void runConsumer(Consumer<Long, String> consumer, long timeoutSeconds) throws InterruptedException {

		long time = System.currentTimeMillis();
		long endTime = time + (timeoutSeconds * 1000);

		consumer.subscribe(Collections.singletonList(KafkaSettings.TOPIC));

		int counter = 0;

		while (time < endTime) {
			ConsumerRecords<Long, String> records = consumer.poll(100);
			records.forEach(record -> System.out.printf("Consumer Record:(%d, %s)\n", record.key(), record.value()));
			counter += records.count();
			time = System.currentTimeMillis();
		}
		System.out.println("recieved " + counter + " messages in " + (timeoutSeconds * 1000) + " sec");
		consumer.unsubscribe();
		consumer.close();
	}
}
