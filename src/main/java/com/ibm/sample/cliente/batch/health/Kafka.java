package com.ibm.sample.cliente.batch.health;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

import org.springframework.stereotype.Component;

import com.ibm.sample.cliente.bff.dto.Cliente;

@Component
public class Kafka implements HealthIndicator {

	
	Logger logger = LoggerFactory.getLogger(Kafka.class);
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
	
	//@Value("${spring.kafka.consumer.ssl.trust-store-password}")
	//private String trustStorePassword;
	
	//@Value("${spring.kafka.consumer.ssl.trust-store-location}")
	//private String trustStoreLocation;
	
	//@Value("${spring.kafka.consumer.ssl.trust-store-type}")
	//private String trustStoreType;
	
	//@Value("${spring.kafka.consumer.security.protocol}")
	//private String securityProtocol;
	
	//@Value("${spring.kafka.consumer.ssl.protocol}")
	//private String sslProtocol;
	
	//@Value("${spring.kafka.properties.sasl.mechanism}")
	//private String saslMechanis;
	
	//@Value("${spring.kafka.properties.sasl.jaas.config}")
	//private String jaasConfig;
	
	KafkaConsumer<String, Cliente> kafka;
	
	private Cliente cliente = new Cliente();
	
	@Override
	public Health health() {

		logger.debug("[health] Kafka");
		try
		{
			logger.debug("Verifying if exist a open connection with Kafka");
			if (kafka ==null)
			{
				logger.debug("Setting the kafka connection properties");
				cliente.setCpf(0L);
				cliente.setNome("CLIENTE SINTETICO - HEALTH CHECK");
				cliente.setNumero(0);
				cliente.setNasc(new java.util.Date());
				
				Properties prop = new Properties();
				prop.setProperty("acks","1");
				prop.setProperty("bootstrap.servers",kafkaURL);
				prop.setProperty("key.serializer",keyse);
				//prop.setProperty("sasl.jaas.config",jaasConfig);
				//prop.setProperty("sasl.mechanism",saslMechanis);
				//prop.setProperty("security.protocol",securityProtocol);
				//prop.setProperty("ssl.enabled.protocols",sslProtocol);
				//prop.setProperty("ssl.truststore.location",trustStoreLocation.substring(5));
				//prop.setProperty("ssl.truststore.password",trustStorePassword);
				//prop.setProperty("ssl.truststore.type",trustStoreType);
				prop.setProperty("value.serializer",valuese);
				prop.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
				prop.setProperty("value.deserializer","org.springframework.kafka.support.serializer.JsonDeserializer");
				prop.setProperty("group.id", "HealthCheck");
				prop.setProperty("spring.json.trusted.packages", "*");
				
				kafka = new KafkaConsumer<>(prop);
				logger.debug("Connected to Kafka");
			}

			logger.debug("Adding topics: " + this.topicoCadastro + " and " + this.topicoDelete + " in the listening pool");
			ArrayList<String> topicos = new ArrayList<>();
			topicos.add(this.topicoCadastro);
			topicos.add(this.topicoDelete);
			kafka.subscribe(topicos);
			logger.debug("Starting Polling");
			ConsumerRecords<String, Cliente> records = kafka.poll(100);
			for (ConsumerRecord<String, Cliente> record: records)
			{
				if (logger.isTraceEnabled() && record.value()!=null)
				{
					logger.trace("Message consumed by HealthCheck: " + record.value().toString());
				}
				
				record.offset();
				
			}
			logger.debug("Commit read messages");
			kafka.commitSync();
			logger.debug("Health Check finished, Kafka Health");
	
		}
		catch (Exception e)
		{
			logger.error("Error to validate Kafka Health: " + e.getMessage(), e);
			return Health.down().withDetail("Kafka is not Health", e.getMessage()).build();
			
		}
		return Health.up().build();
	}
	
	
	
}
