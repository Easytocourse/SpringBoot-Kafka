package com.easytocourse.service;

import java.util.List;
import java.util.stream.IntStream;

import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.easytocourse.dao.EmployeeRepository;
import com.easytocourse.model.Employee;

@Service
public class Employeeservice {

	@Autowired
	EmployeeRepository emprepo;

	private static final String topic = "topic";
	private static final String emptopic = "emptopic";

	@Autowired
	private KafkaTemplate<String, String> template;

	@Autowired
	private KafkaTemplate<String, Employee> emptemplate;

	private static final Logger logger = LoggerFactory.getLogger(Employeeservice.class);

	public List<Employee> getallactionitems() {
		return emprepo.findAll();
	}

	public String sendtokafka(String message) {

		ListenableFuture<SendResult<String, String>> future = this.template.send(topic, message);

		future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

			@Override
			public void onSuccess(SendResult<String, String> result) {
				logger.info("Sent message: " + message + " with offset: " + result.getRecordMetadata().offset());

			}

			@Override
			public void onFailure(Throwable ex) {
				logger.error("Unable to send message : " + message, ex);
			}
		});

		return message;
	}

	@KafkaListener(topics = topic, groupId = "mygroupid")
	public void consume(String message) {
		logger.info(String.format("Message recieved -> %s", message));
	}

	public Employee saveaction(Employee emp) {

		Employee savedemp = emprepo.save(emp);

		ListenableFuture<SendResult<String, Employee>> future = this.emptemplate.send(emptopic, savedemp);

		future.addCallback(new ListenableFutureCallback<SendResult<String, Employee>>() {

			@Override
			public void onSuccess(SendResult<String, Employee> result) {
				logger.info("Sent Employee: " + emp + " with offset: " + result.getRecordMetadata().offset());

			}

			@Override
			public void onFailure(Throwable ex) {
				logger.error("Unable to send Employee : " + emp, ex);
			}
		});

		return savedemp;

	}

	@KafkaListener(topics = emptopic, groupId = "empgroupid")
	public void consumeemp(Employee emp) {
		logger.info(String.format("Employee recieved -> %s", emp));
	}

	/*
	 * Testing with one (myfirstopic) topic by partioning into 3 and creating 3
	 * consumers in same group
	 */

	@Bean
	public NewTopic compactTopicExample() {
		return TopicBuilder.name("myfirsttopic").partitions(3).build();
	}

	public void sendingloop() {
		IntStream.range(0, 100).forEach(i -> this.template.send("myfirsttopic", "data value->" + i));
		logger.info("stream sending finished");
	}

	@KafkaListener(topics = "myfirsttopic", groupId = "loopgroupid")
	public void consumingloop1(String data) {
		logger.info(String.format("consumingloop1 -> %s", data));
	}

	@KafkaListener(topics = "myfirsttopic", groupId = "loopgroupid")
	public void consumingloop2(String data) {
		logger.info(String.format("consumingloop2 -> %s", data));
	}

	@KafkaListener(topics = "myfirsttopic", groupId = "loopgroupid")
	public void consumingloop3(String data) {
		logger.info(String.format("consumingloop3 -> %s", data));
	}

	// here i passed 100 values some of them are processed by cosumingloop1 &
	// consumingloop2(shared among them)
	/*
	 * Testing with one (myfirstopic) topic by partioning into 3 and creating 3
	 * consumers in same group end
	 */

	/*
	 * Testing consumers in different group accessing the same topic creating a
	 * single topic and creating the two consumers with different groupid
	 * 
	 * Begin
	 */

	@Bean
	public NewTopic singletopic() {
		return TopicBuilder.name("singletopic").partitions(3).build();
	}

	public void sendingtosingletopic() {
		IntStream.range(0, 100).forEach(i -> this.template.send("singletopic", "data value->" + i));
		logger.info("stream sending finished");
	}

	@KafkaListener(topics = "singletopic", groupId = "diffgroupid1")
	public void consumingloop4(String data) {
		logger.info(String.format("consumingloop4 -> %s", data));
	}

	@KafkaListener(topics = "singletopic", groupId = "diffgroupid2")
	public void consumingloop5(String data) {
		logger.info(String.format("consumingloop5 -> %s", data));
	}

	/*
	 * consumingloop4 -> data value->0 consumingloop5 -> data value->0
	 * consumingloop5 -> data value->1 consumingloop4 -> data value->1 . . .....
	 * both consumers got the same messages( this will happens only if consumers are
	 * in the different groups) End
	 */

	/*
	 * lets try two different topics and one single consumer
	 */
	@Bean
	public NewTopic multi1() {
		return TopicBuilder.name("multi1").partitions(1).build();
	}

	@Bean
	public NewTopic multi2() {
		return TopicBuilder.name("multi2").partitions(1).build();
	}

	public void sendingtomultitopic() {
		IntStream.range(0, 100).forEach(i -> this.template.send("multi1", "mutli1 data value->" + i));
		IntStream.range(0, 100).forEach(i -> this.template.send("multi2", "multi2 data value->" + i));
		logger.info("stream sending finished");
	}

	@KafkaListener(topics = {"multi1","multi2"}, groupId = "diffgroupid3")
	public void consumingfrommultitopics(String data) {
		logger.info(String.format("consumingfromtwotopics -> %s", data));
	}
	/*
	 * END
	 */

	/*
	 * Lets create a reply to another topics once it is received from a consumer, it
	 * will send received msg to another topic Begin
	 */

	@Bean
	public NewTopic fromtopic() {
		return TopicBuilder.name("fromtopic").partitions(1).build();
	}

	@Bean
	public NewTopic totopic() {
		return TopicBuilder.name("totopic").partitions(1).build();
	}

	public void sendingtofromtopic() {
		IntStream.range(0, 10).forEach(i -> this.template.send("fromtopic", "from data value->" + i));

		logger.info("stream sending finished");
	}

	@KafkaListener(topics = "fromtopic",groupId = "diffgroupid4")  //receving topic
	@SendTo("totopic")                                            //sending to another topic
	public void listenAndReply(String message) {
		
		logger.info("ListenAndReply [{}]", "in totopic-->"+message);

	}

}
