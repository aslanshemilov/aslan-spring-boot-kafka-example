package com.aslan.app.controller;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.aslan.app.model.DataModel;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

@RestController
@RequestMapping("/api")
public class ApiController {
    //private final Logger logger = LoggerFactory.getLogger(getClass());
	private Logger logger = LoggerFactory.getLogger(ApiController.class.getName());
	private ObjectMapper objMapper;
	private Gson gson;
	
	@Value("${spring.application.name}")
	private String appname;
	
	private String bootstrapServers = "127.0.0.1:9092";
	
	@Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;
	
	public ApiController() {
		this.objMapper = new ObjectMapper();
		this.objMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
		this.gson = new Gson();
	}
	
	
	@GetMapping("")
	public String Default() {
		return "This is API controller. " + appname;
	}
	
	@RequestMapping(path = {"/health", "/health.html"}, method = RequestMethod.GET, produces = {MediaType.TEXT_HTML_VALUE, "text/html"})
	public String healthyText() {
		return "health";
	}
	
	@RequestMapping(path = {"/start", "/starting"}, 
			method = {RequestMethod.GET}, 
			produces = {MediaType.APPLICATION_JSON_VALUE, "application/json; charset=utf-8"}, 
			consumes = {MediaType.ALL_VALUE})
	@ResponseBody
	public ResponseEntity<?> start() {
		Map<String, String> map = new HashMap<String, String>();
		
		return new ResponseEntity<>(map, HttpStatus.OK);
	}
	
	@RequestMapping(path = {"/process"}, 
			method = {RequestMethod.POST}, 
			produces = {MediaType.APPLICATION_JSON_VALUE, "application/json; charset=utf-8"}, 
			consumes = {MediaType.ALL_VALUE})
	public ResponseEntity<?> process(
			@RequestHeader Map<?, ?> headers, 
			@RequestParam(value="id", required=false, defaultValue="0") String id, 
			@RequestBody(required=true) Map<?, ?> req) {
		Map<String, String> map = new HashMap<String, String>();
		
		return new ResponseEntity<>(map, HttpStatus.OK);
	}
	
	@SuppressWarnings("unchecked")
	@RequestMapping(path = {"/kafka/basic-producer"}, 
			method = {RequestMethod.GET}, 
			produces = {MediaType.APPLICATION_JSON_VALUE, "application/json; charset=utf-8"}, 
			consumes = {MediaType.ALL_VALUE})
	@ResponseBody
	public ResponseEntity<?> basicProducer() {
		logger.info("ApiController::basicProducer(): Start");
		JSONObject jo = new JSONObject();
		String topic = "first_topic";
		try {
			// create message object		
			DataModel dataModel = new DataModel();
			dataModel.setName("message");
			dataModel.setMessage("message-1");
			
			JSONObject msg = new JSONObject();
			msg.put("message-1", "Aslan is very good guy");
			//dataModel.setDt_model(msg);
			
			// publish message using KafkaTemplate
			ListenableFuture<SendResult<Object, Object>> result = kafkaTemplate.send(topic, dataModel);
			
			result.addCallback(new ListenableFutureCallback<SendResult<Object, Object>>() {

		        @Override
		        public void onSuccess(SendResult<Object, Object> result) {
		        	logger.info("Sent message=[" + dataModel + 
		              "] with offset=[" + result.getRecordMetadata().offset() + "]");
		        }
		        @Override
		        public void onFailure(Throwable ex) {
		        	logger.error("Unable to send message=[" 
		              + dataModel + "] due to : " + ex.getMessage());
		        }
		    });
	        
		} catch(Exception ex) {
			//logger.error("ApiController::basicProducer(): Exeption: {}", gson.toJson(ex.getMessage()));
			//Map<String, Object> objMap = objMapper.readValue(gson.toJson(ex.getMessage()), Map.class);
			
			logger.error("ApiController::basicProducer(): Exeption: {}", ex.getMessage());
		} finally {
			logger.info("ApiController::basicProducer(): End");
		}
				
		return new ResponseEntity<>(jo, HttpStatus.OK);
	}
	
	@RequestMapping(path = {"/kafka/basic-consumer"}, 
			method = {RequestMethod.GET}, 
			produces = {MediaType.APPLICATION_JSON_VALUE, "application/json; charset=utf-8"}, 
			consumes = {MediaType.ALL_VALUE})
	@ResponseBody
	public ResponseEntity<?> basicConsumer() {
		logger.info("ApiController::basicConsumer(): Start");
		JSONObject jo = new JSONObject();
		String topic = "first_topic";
		String groupId = "my-fourth-application";
		try {		
			// create consumer configs
	        Properties properties = new Properties();
	        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
	        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
	        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	        
	     // create consumer
	        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

	        // subscribe consumer to our topic(s)
	        consumer.subscribe(Arrays.asList(topic));

	        // poll for new data
	        while(true){
	            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0

	            for (ConsumerRecord<String, String> record : records){
	                logger.info("Key: " + record.key() + ", Value: " + record.value());
	                logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
	            }
	        }
	        
		} catch(Exception ex) {
			//logger.error("ApiController::basicProducer(): Exeption: {}", gson.toJson(ex.getMessage()));
			//Map<String, Object> objMap = objMapper.readValue(gson.toJson(ex.getMessage()), Map.class);
			
			logger.error("ApiController::basicConsumer(): Exeption: {}", ex.getMessage());
		} finally {
			logger.info("ApiController::basicConsumer(): End");
		}
				
		return new ResponseEntity<>(jo, HttpStatus.OK);
	}

} //end class
