package com.ibm.sample.cliente.batch.health;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.ibm.sample.cliente.bff.dto.Cliente;

@Component
public class Kafka implements HealthIndicator {

	
	@Value("${cliente-kafka-topico}")
	private String topicoCadastro; 
	
	@Value("${delete-cliente-kafka-topico}")
	private String topicoDelete; 
	
	@Value("${spring.kafka.consumer.bootstrap-servers}")
	private String kafkaURL;
	
	
	@Value("${spring.kafka.producer.key-serializer}")
	private String keyse;
	
	@Value("${spring.kafka.producer.value-serializer}")
	private String valuese;
	
	@Value("${spring.kafka.consumer.ssl.trust-store-password}")
	private String trustStorePassword;
	
	@Value("${spring.kafka.consumer.ssl.trust-store-location}")
	private String trustStoreLocation;
	
	@Value("${spring.kafka.consumer.ssl.trust-store-type}")
	private String trustStoreType;
	
	@Value("${spring.kafka.consumer.security.protocol}")
	private String securityProtocol;
	
	@Value("${spring.kafka.consumer.ssl.protocol}")
	private String sslProtocol;
	
	@Value("${spring.kafka.properties.sasl.mechanism}")
	private String saslMechanis;
	
	@Value("${spring.kafka.properties.sasl.jaas.config}")
	private String jaasConfig;
	
	KafkaConsumer<String, Cliente> kafka;
	
	private Cliente cliente = new Cliente();
	
	@Override
	public Health health() {
		int errorCode = 0;
		try
		{
			if (kafka ==null)
			{
				
				cliente.setCpf(0L);
				cliente.setNome("CLIENTE SINTETICO - HEALTH CHECK");
				cliente.setNumero(0);
				cliente.setNasc(new java.util.Date());
				
				Properties prop = new Properties();
				prop.setProperty("acks","1");
				prop.setProperty("bootstrap.servers",kafkaURL);
				prop.setProperty("key.serializer",keyse);
				prop.setProperty("sasl.jaas.config",jaasConfig);
				prop.setProperty("sasl.mechanism",saslMechanis);
				prop.setProperty("security.protocol",securityProtocol);
				prop.setProperty("ssl.enabled.protocols",sslProtocol);
				prop.setProperty("ssl.truststore.location",trustStoreLocation.substring(5));
				prop.setProperty("ssl.truststore.password",trustStorePassword);
				prop.setProperty("ssl.truststore.type",trustStoreType);
				prop.setProperty("value.serializer",valuese);
				prop.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
				prop.setProperty("value.deserializer","org.springframework.kafka.support.serializer.JsonDeserializer");
				prop.setProperty("group.id", "HealthCheck");
				kafka = new KafkaConsumer<>(prop);
			}

			
			ArrayList<String> topicos = new ArrayList<>();
			topicos.add(this.topicoCadastro);
			topicos.add(this.topicoDelete);
			kafka.subscribe(topicos);
			ConsumerRecords<String, Cliente> records = kafka.poll(100);
			for (ConsumerRecord<String, Cliente> record: records)
			{
				record.value();
				
			}
			kafka.commitSync();
			
	
		}
		catch (Exception e)
		{
			errorCode=1;
			e.printStackTrace();
			//System.out.println("Kafka não esta saudável: " + e.getMessage());
			return Health.down().withDetail("Kafka Não saudável", e.getMessage()).build();
			
		}
		return Health.up().build();
	}
	
	
	
}