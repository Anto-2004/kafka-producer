package com.kafka.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.kafka.serialize.CustomSerializer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * This class is used to create a producer class with Kafka
 * 
 * @author Antonino Faraci
 *
 */
public class ProducerKafka {

	private KafkaProducer<String, String> producer;
	private CustomSerializer<String> serializer;

	// create Producer properties
	private Properties properties;

	private String bootstrapServers;
	private String topic;

	/**
	 * In this constructor init the server port and the server host
	 * 
	 * @param host The host of the server Kafka
	 * @param port The port of the server Kafka
	 */
	public ProducerKafka(String host, int port) {

		bootstrapServers = host + ":" + port;

		serializer = new CustomSerializer<String>();

		properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer.getName());
		this.producer = new KafkaProducer<>(properties);

	}

	/**
	 * This method is used to send a custom object class message
	 * 
	 * @param <T>
	 * @param obj       The object to send via Kafka
	 * @param classType The type of class to send
	 * @throws Exception Launch exception if the topic is not setted correctly
	 */
	@SuppressWarnings("resource")
	public <T> void send(T obj, Class<T> classType) throws Exception {
		if (this.topic != null && !this.topic.isEmpty()) {

			CustomSerializer<T> serializerCustom = new CustomSerializer<T>();

			properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializerCustom.getName());

			KafkaProducer<String, T> producerCustom = new KafkaProducer<String, T>(properties);

			ProducerRecord<String, T> producerRecord = new ProducerRecord<>(topic, obj);
			producerCustom.send(producerRecord);

			producerCustom.flush();
			producerCustom.close();

		} else {
			throw new Exception("The server topic has not been set");
		}

	}

	/**
	 * This method is used to send a simple text string
	 * 
	 * @param msg The message string to sent via Kafka
	 * @throws Exception Launch this exception if the topic is not setted
	 */
	public void send(String msg) throws Exception {
		if (this.topic != null && !this.topic.isEmpty()) {

			ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, msg);
			this.producer.send(producerRecord);

		} else {
			throw new Exception("The server topic has not been set");
		}
	}

	/**
	 * This method is used to set the topic to use
	 * 
	 * @param topic The name of the topic to use
	 */
	public void setTopic(String topic) {
		this.topic = topic;
	}

	/**
	 * This method is used to flush the producer object
	 */
	public void flush() {
		// flush data - synchronous
		producer.flush();
	}

	/**
	 * This method is used to close the producer connection
	 */
	public void close() {
		// close producer
		producer.close();
	}

}
