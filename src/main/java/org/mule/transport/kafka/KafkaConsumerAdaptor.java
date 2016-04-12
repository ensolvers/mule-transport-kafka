/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.mule.transport.kafka;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.mule.api.endpoint.ImmutableEndpoint;

import com.google.common.collect.Lists;

/**
 * A simplified adaptor class that is used by {@link KafkaMessageReceiver} and
 * {@link KafkaMessageRequester}
 * 
 * @author Esteban Robles Luna <esteban.roblesluna@gmail.com>
 */
class KafkaConsumerAdaptor {

	private final ExecutorService service;
	private volatile KafkaConsumer<String, String> consumer;
	private final KafkaConsumerAdaptorListener listener;
	
	public KafkaConsumerAdaptor(KafkaConsumerAdaptorListener listener) {
		this.service = Executors.newSingleThreadExecutor();
		this.listener = listener;
	}

	public void initialize(KafkaConnector connector, ImmutableEndpoint endpoint) {
		String topic = endpoint.getEndpointURI().getAddress();
		
		String groupId = (String) endpoint.getProperties().get("groupId");

		String enableAutoCommit = (String) endpoint.getProperties().get("enableAutoCommit");
		String autoCommitIntervalMS = (String) endpoint.getProperties().get("autoCommitIntervalMS");
		String sessionTimeoutMS = (String) endpoint.getProperties().get("sessionTimeoutMS");
		String keyDeserializer = (String) endpoint.getProperties().get("keyDeserializer");
		String valueDeserializer = (String) endpoint.getProperties().get("valueDeserializer");

		groupId = (StringUtils.isBlank(groupId)) ? "group" + ((int)(Math.random() * 100000)) : groupId;
		enableAutoCommit = (StringUtils.isBlank(enableAutoCommit)) ? "true" : enableAutoCommit;
		autoCommitIntervalMS = (StringUtils.isBlank(autoCommitIntervalMS)) ? "1000" : autoCommitIntervalMS;
		sessionTimeoutMS = (StringUtils.isBlank(sessionTimeoutMS)) ? "30000" : sessionTimeoutMS;
		keyDeserializer = (StringUtils.isBlank(keyDeserializer)) ? "org.apache.kafka.common.serialization.StringDeserializer" : keyDeserializer;
		valueDeserializer = (StringUtils.isBlank(valueDeserializer)) ? "org.apache.kafka.common.serialization.StringDeserializer" : valueDeserializer;
		
		this.initialize(
				Lists.newArrayList(topic), 
				connector.getBootstrapServers(),
				groupId,
				enableAutoCommit,
				autoCommitIntervalMS,
				sessionTimeoutMS,
				keyDeserializer,
				valueDeserializer);
	}

	public void initialize(
			List<String> topics, 
			String boostrapServers, 
			String groupId,
			String enableAutoCommit,
			String autoCommitInterval,
			String sessionTimeout,
			String keyDeserializer,
			String valueDeserializer) {
		
 		Properties props = new Properties();
		props.put("bootstrap.servers", boostrapServers);
		
		props.put("group.id", groupId);
		props.put("enable.auto.commit", enableAutoCommit);
		props.put("auto.commit.interval.ms", autoCommitInterval);
		props.put("session.timeout.ms", sessionTimeout);
		props.put("key.deserializer", keyDeserializer);
		props.put("value.deserializer", valueDeserializer);
		
		consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(topics);
		
		this.service.execute(new Runnable() {
			@Override
			public void run() {
				while (true) {
					try {
						ConsumerRecords<String, String> records = consumer.poll(1000);
						for (ConsumerRecord<String, String> record : records) {
							listener.onRecord(record);
						}
						Thread.sleep(5);
					} catch (Exception e) {
					}
				}
			}
		});
	}
}
