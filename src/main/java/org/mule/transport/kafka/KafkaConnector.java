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

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.mule.api.MuleContext;
import org.mule.api.MuleException;
import org.mule.api.lifecycle.InitialisationException;
import org.mule.transport.AbstractConnector;

/**
 * The kafka connector
 * 
 * @author Esteban Robles Luna <esteban.roblesluna@gmail.com>
 */
public class KafkaConnector extends AbstractConnector {

	public static final String KAFKA = "kafka";

	public static final int DEFAULT_PRODUCER_RETRIES = 0;
	public static final int DEFAULT_PRODUCER_BATCH_SIZE = 16384;
	public static final int DEFAULT_PRODUCER_BUFFER_MEMORY = 33554432;
	public static final int DEFAULT_PRODUCER_LINGER_MS = 1;
	public static final String DEFAULT_PRODUCER_ACKS = "all";
	public static final String DEFAULT_PRODUCER_KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
	public static final String DEFAULT_PRODUCER_VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
	public static final String DEFAULT_PRODUCER_COMPRESSION_TYPE = "gzip";

	private volatile KafkaProducer<String, String> producer;

	private String bootstrapServers;
	
	private String producerAcks;
	private int producerRetries;
	private int producerBatchSize;
	private int producerLingerMS;
	private int producerBufferMemory;
	private String producerKeySerializer;
	private String producerValueSerializer;
	private String producerCompressionType;

	public KafkaConnector(MuleContext context) {
		super(context);

		this.producerRetries = DEFAULT_PRODUCER_RETRIES;
		this.producerBatchSize = DEFAULT_PRODUCER_BATCH_SIZE;
		this.producerBufferMemory = DEFAULT_PRODUCER_BUFFER_MEMORY;
		this.producerLingerMS = DEFAULT_PRODUCER_LINGER_MS;
		this.producerAcks = DEFAULT_PRODUCER_ACKS;
		this.producerKeySerializer = DEFAULT_PRODUCER_KEY_SERIALIZER;
		this.producerValueSerializer = DEFAULT_PRODUCER_VALUE_SERIALIZER;
		this.producerCompressionType = DEFAULT_PRODUCER_COMPRESSION_TYPE;
	}

	public void doInitialise() throws InitialisationException {
	}

	@Override
	public void doConnect() throws Exception {
		Properties config = new Properties();

	    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
	    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, this.producerKeySerializer);
	    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, this.producerValueSerializer);
	    config.put(ProducerConfig.ACKS_CONFIG, this.producerAcks);
	    config.put(ProducerConfig.LINGER_MS_CONFIG, this.producerLingerMS);
	    config.put(ProducerConfig.BATCH_SIZE_CONFIG, this.producerBatchSize);
	    config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, this.producerBufferMemory);
	    config.put(ProducerConfig.RETRIES_CONFIG, this.producerRetries);
	    config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, this.producerCompressionType);

		this.producer = new KafkaProducer<String, String>(config);
	}

	@Override
	protected void doStart() throws MuleException {
	}

	@Override
	protected void doStop() throws MuleException {
	}

	@Override
	public void doDisconnect() throws Exception {
	}

	@Override
	public void doDispose() {
		if (this.producer != null) {
			this.producer.close();
		}
	}
	
	void send(final ProducerRecord<String, String> record) {
		producer.send(record);
	}
	
	public String getProtocol() {
		return KAFKA;
	}

	// PROPERTIES
	public String getBootstrapServers() {
		return bootstrapServers;
	}

	public void setBootstrapServers(String bootstrapServers) {
		this.bootstrapServers = bootstrapServers;
	}

	public String getProducerAcks() {
		return producerAcks;
	}

	public void setProducerAcks(String producerAcks) {
		this.producerAcks = producerAcks;
	}

	public int getProducerRetries() {
		return producerRetries;
	}

	public void setProducerRetries(int producerRetries) {
		this.producerRetries = producerRetries;
	}

	public int getProducerBatchSize() {
		return producerBatchSize;
	}

	public void setProducerBatchSize(int producerBatchSize) {
		this.producerBatchSize = producerBatchSize;
	}

	public int getProducerLingerMS() {
		return producerLingerMS;
	}

	public void setProducerLingerMS(int producerLingerMS) {
		this.producerLingerMS = producerLingerMS;
	}

	public int getProducerBufferMemory() {
		return producerBufferMemory;
	}

	public void setProducerBufferMemory(int producerBufferMemory) {
		this.producerBufferMemory = producerBufferMemory;
	}

	public String getProducerKeySerializer() {
		return producerKeySerializer;
	}

	public void setProducerKeySerializer(String producerKeySerializer) {
		this.producerKeySerializer = producerKeySerializer;
	}

	public String getProducerValueSerializer() {
		return producerValueSerializer;
	}

	public void setProducerValueSerializer(String producerValueSerializer) {
		this.producerValueSerializer = producerValueSerializer;
	}

	public String getProducerCompressionType() {
		return producerCompressionType;
	}

	public void setProducerCompressionType(String producerCompressionType) {
		this.producerCompressionType = producerCompressionType;
	}
}
