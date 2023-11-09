package com.ibm.sample.cliente.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;


import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;

import java.net.URI;
import java.util.HashMap;

import com.ibm.sample.HttpHeaderInjectAdapter;
import com.ibm.sample.PropagacaoContexto;
import com.ibm.sample.cliente.batch.dto.RetornoCliente;
import com.ibm.sample.cliente.bff.dto.Cliente;

@Service
public class CadastraClienteService extends PropagacaoContexto {

	Logger logger = LoggerFactory.getLogger(CadastraClienteService.class);

	@Value("${cliente-rest.url}")
	private String urlClienteRest; 
	
	@Autowired
	Tracer tracer;

	@KafkaListener(topics = "${cliente-kafka-topico}")
	public void cadastraCliente(Cliente cliente,  @Headers MessageHeaders headers)
	{
		
		logger.debug("[cadastraCliente] " + cliente);
		
		if (cliente==null || cliente.getCpf()==0L) //health check
		{
			logger.debug("CPF 0, sintect transaction, it will be ignored! ");
			return;
		}
		Span span = this.startConsumerSpan("consomeMensagemCadastroCliente", headers, tracer);
		try
		{
			span.setTag("payload", cliente.toString());
			RestTemplate clienteRest = new RestTemplate();
			logger.debug("Invoking Rest API to store the customer data in the database");
			HttpHeaders httpHeaders = new HttpHeaders();
			HttpHeaderInjectAdapter h1 = new HttpHeaderInjectAdapter(httpHeaders);
			tracer.inject(span.context(), Format.Builtin.HTTP_HEADERS,h1);
			HttpEntity<Cliente> entity = new HttpEntity<>(cliente, h1.getHeaders());
			RetornoCliente retorno = clienteRest.postForObject(urlClienteRest,entity, RetornoCliente.class);
			logger.info("Customer Record return by API: " + retorno.getMensagem() + ", Customer: " + retorno.getCliente().toString());
			//System.out.println("Resultado " + retorno.getMensagem());
		}
		catch (Exception e)
		{
			span.log("Error: " + e.getMessage() );
			span.setTag("error",true);
			logger.error("Error to save the customer: " + cliente.toString() + ", error: " + e.getMessage(), e);
		}
		finally{
			span.finish();
		}
	}

	@KafkaListener(topics = "${cliente-update-topico}")
	public void atualizaCLente(Cliente cliente,  @Headers MessageHeaders headers)
	{
		
		logger.debug("[atualizaCliente] " + cliente);
		
		if (cliente==null || cliente.getCpf()==0L) //health check
		{
			logger.debug("CPF 0, sintect transaction, it will be ignored! ");
			return;
		}
		Span span = this.startConsumerSpan("consomeMensagemAtualizacaoCliente", headers, tracer);
		try
		{
			span.setTag("payload", cliente.toString());
			RestTemplate clienteRest = new RestTemplate();
			logger.debug("Invoking Rest API to store the customer data in the database");
			HttpHeaders httpHeaders = new HttpHeaders();
			HttpHeaderInjectAdapter h1 = new HttpHeaderInjectAdapter(httpHeaders);
			tracer.inject(span.context(), Format.Builtin.HTTP_HEADERS,h1);
			HttpEntity<Cliente> entity = new HttpEntity<>(cliente, h1.getHeaders());
			RetornoCliente retorno = clienteRest.postForObject(urlClienteRest,entity, RetornoCliente.class);
			logger.info("Customer Record return by API: " + retorno.getMensagem() + ", Customer: " + retorno.getCliente().toString());
			//System.out.println("Resultado " + retorno.getMensagem());
		}
		catch (Exception e)
		{
			span.log("Error: " + e.getMessage() );
			span.setTag("error",true);
			logger.error("Error to save the customer: " + cliente.toString() + ", error: " + e.getMessage(), e);
		}
		finally{
			span.finish();
		}
	}

	
	@KafkaListener(topics = "${delete-cliente-kafka-topico}", groupId = "Delete-Cliente")
	public void excluiCliente(Cliente cliente,  @Headers MessageHeaders headers)
	{
		logger.debug("[excluiCliente] " + cliente);
		
		if (cliente==null || cliente.getCpf()==0L) //health check
		{
			logger.debug("CPF 0, sintect transaction, it will be ignored!");
			return;
		}
		Span span = this.startConsumerSpan("consomeMensagemExclusaoCliente", headers, tracer);
		try
		{
			HttpHeaders httpHeaders = new HttpHeaders();
			HttpHeaderInjectAdapter h1 = new HttpHeaderInjectAdapter(httpHeaders);
			tracer.inject(span.context(), Format.Builtin.HTTP_HEADERS,h1);
			
			HttpEntity<String> entity = new HttpEntity<>( h1.getHeaders());
			RestTemplate clienteRest = new RestTemplate();
			logger.debug("Invoking Rest API to delete the customer");
			URI url = new URI(urlClienteRest + "/" + cliente.getCpf());
			clienteRest.exchange(url,HttpMethod.DELETE,entity,String.class);
			//clienteRest.
			logger.debug("Customer deleted sucessfully! customer: " + cliente.toString());
			
		}
		catch (Exception e)
		{
			span.log("Error: " + e.getMessage() );
			span.setTag("error",true);
			logger.error("Error to delete customer: " + cliente.toString() +", error: " + e.getMessage() ,e);
		}
		finally {
			span.finish();
		}
	}
	
}
