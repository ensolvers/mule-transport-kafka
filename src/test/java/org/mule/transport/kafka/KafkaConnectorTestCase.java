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

import junit.framework.Assert;

import org.junit.Test;
import org.mule.api.transport.Connector;
import org.mule.transport.AbstractConnectorTestCase;

/**
 * A test for the kafka connector
 * 
 * @author Esteban Robles Luna <esteban.roblesluna@gmail.com>
 */
public class KafkaConnectorTestCase extends AbstractConnectorTestCase
{
    @Override
    public Connector createConnector() throws Exception
    {
        KafkaConnector connector = new KafkaConnector(muleContext);
        connector.setName("Test");
        
        connector.setBootstrapServers("localhost:1029");
        
        connector.setProducerAcks("all");
        connector.setProducerRetries(1);
        connector.setProducerBatchSize(2);
        connector.setProducerLingerMS(3);
        connector.setProducerBufferMemory(4);
        connector.setProducerKeySerializer("org.apache.kafka.common.serialization.StringSerializer");
        connector.setProducerValueSerializer("org.apache.kafka.common.serialization.StringSerializer");
        connector.setProducerCompressionType("gzip");

        return connector;
    }

    @Override
    public String getTestEndpointURI()
    {
    	return "kafka://topic1";
    }

    @Override
    public Object getValidMessage() throws Exception
    {
        return "valid message";
    }

    @Test
    public void customProperties() throws Exception
    {
    	KafkaConnector connector = (KafkaConnector) this.getConnector();
    	
    	Assert.assertEquals("localhost:1029", connector.getBootstrapServers());
    	Assert.assertEquals("all", connector.getProducerAcks());
    	Assert.assertEquals(1, connector.getProducerRetries());
    	Assert.assertEquals(2, connector.getProducerBatchSize());
    	Assert.assertEquals(3, connector.getProducerLingerMS());
    	Assert.assertEquals(4, connector.getProducerBufferMemory());
    	Assert.assertEquals("org.apache.kafka.common.serialization.StringSerializer", connector.getProducerKeySerializer());
    	Assert.assertEquals("org.apache.kafka.common.serialization.StringSerializer", connector.getProducerValueSerializer());
    	Assert.assertEquals("gzip", connector.getProducerCompressionType());
    }
}
