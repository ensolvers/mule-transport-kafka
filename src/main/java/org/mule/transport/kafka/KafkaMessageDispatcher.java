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

import org.apache.kafka.clients.producer.ProducerRecord;
import org.mule.api.MuleEvent;
import org.mule.api.MuleMessage;
import org.mule.api.endpoint.OutboundEndpoint;
import org.mule.transport.AbstractMessageDispatcher;

/**
 * The dispatcher for kafka messages.
 * 
 * @author Esteban Robles Luna <esteban.roblesluna@gmail.com>
 */
public class KafkaMessageDispatcher extends AbstractMessageDispatcher
{
	private final String topic;
	
    public KafkaMessageDispatcher(OutboundEndpoint endpoint)
    {
        super(endpoint);

        this.topic = endpoint.getEndpointURI().getAddress();
    }

    @Override
    public void doConnect() throws Exception
    {
    }

    @Override
    public void doDisconnect() throws Exception
    {
    }

    @Override
    public void doDispatch(MuleEvent event) throws Exception {
    	basicSend(event);
    }

    @Override
    public MuleMessage doSend(MuleEvent event) throws Exception {
    	return basicSend(event);
    }
    
    public MuleMessage basicSend(MuleEvent event) throws Exception {
    	String key = Integer.valueOf((int)Math.round(Math.random() * 100)).toString();
    	String value = event.getMessageAsString();

    	ProducerRecord<String, String> record = new ProducerRecord<String, String>(
    			this.topic, 
    			key, 
    			value);
    	
    	KafkaConnector connector = (KafkaConnector) this.getConnector();
    	connector.send(record);
    	return event.getMessage();
    }

}

