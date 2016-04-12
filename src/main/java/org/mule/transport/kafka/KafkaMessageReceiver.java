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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.mule.DefaultMuleMessage;
import org.mule.api.MuleEvent;
import org.mule.api.MuleException;
import org.mule.api.MuleMessage;
import org.mule.api.construct.FlowConstruct;
import org.mule.api.endpoint.InboundEndpoint;
import org.mule.api.execution.ExecutionCallback;
import org.mule.api.execution.ExecutionTemplate;
import org.mule.api.lifecycle.CreateException;
import org.mule.api.transport.Connector;
import org.mule.transport.AbstractMessageReceiver;

/**
 * The receiver for kafka messages.
 * 
 * @author Esteban Robles Luna <esteban.roblesluna@gmail.com>
 */
public class KafkaMessageReceiver extends AbstractMessageReceiver implements KafkaConsumerAdaptorListener {

	private volatile KafkaConsumerAdaptor consumer;

	public KafkaMessageReceiver(
			Connector connector,
			FlowConstruct flowConstruct, 
			InboundEndpoint endpoint)
			throws CreateException {

		super(connector, flowConstruct, endpoint);
	}

	@Override
	protected void doConnect() throws Exception {
		super.doConnect();
		
		this.consumer = new KafkaConsumerAdaptor(this);
	}

	@Override
	protected void doStart() throws MuleException {
		super.doStart();

		KafkaConnector connector = (KafkaConnector) this.connector;
		this.consumer.initialize(connector, this.endpoint);
	}

	public void processMessage(final MuleMessage message) {
		ExecutionTemplate<MuleEvent> executionTemplate = createExecutionTemplate();

		try {
			executionTemplate.execute(new ExecutionCallback<MuleEvent>() {
				@Override
				public MuleEvent process() throws Exception {
					return routeMessage(message);
				}
			});
		} catch (Exception e) {
			this.getEndpoint().getMuleContext().getExceptionListener().handleException(e);
		}		
	}

	@Override
	public void onRecord(ConsumerRecord<String, String> record) {
		MuleMessage message = new DefaultMuleMessage(
				record.value(),
				(Map<String, Object>) null, 
				this.getEndpoint().getMuleContext());
		
		this.processMessage(message);
	}
}
