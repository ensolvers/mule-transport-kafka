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

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.mule.DefaultMuleMessage;
import org.mule.api.MuleMessage;
import org.mule.api.endpoint.InboundEndpoint;
import org.mule.transport.AbstractMessageRequester;

/**
 * The requester of kafka messages.
 * 
 * @author Esteban Robles Luna <esteban.roblesluna@gmail.com>
 */
public class KafkaMessageRequester extends AbstractMessageRequester implements KafkaConsumerAdaptorListener {

	private final KafkaConsumerAdaptor consumer;

	private volatile CountDownLatch latch;
	private volatile ConsumerRecord<String, String> lastRecord;

	public KafkaMessageRequester(InboundEndpoint endpoint) {
		super(endpoint);
		
		this.consumer = new KafkaConsumerAdaptor(this);
		
		KafkaConnector connector = (KafkaConnector) this.connector;
		this.consumer.initialize(connector, this.endpoint);
	}

	@Override
	protected MuleMessage doRequest(long timeout) throws Exception {
		this.latch = new CountDownLatch(1);
		this.lastRecord = null;
		this.latch.await(timeout, TimeUnit.MILLISECONDS);
		
		if (lastRecord == null) {
			return null;
		} else {
			MuleMessage message = new DefaultMuleMessage(
					this.lastRecord.value(),
					(Map<String, Object>) null, 
					this.getEndpoint().getMuleContext());

			return message;
		}
	}

	@Override
	public void onRecord(ConsumerRecord<String, String> record) {
		this.lastRecord = record;
		this.latch.countDown();
	}
}
