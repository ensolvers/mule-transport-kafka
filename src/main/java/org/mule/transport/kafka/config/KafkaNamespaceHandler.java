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
package org.mule.transport.kafka.config;

import org.mule.config.spring.factories.InboundEndpointFactoryBean;
import org.mule.config.spring.factories.OutboundEndpointFactoryBean;
import org.mule.config.spring.handlers.AbstractMuleNamespaceHandler;
import org.mule.config.spring.parsers.MuleDefinitionParser;
import org.mule.config.spring.parsers.specific.endpoint.TransportEndpointDefinitionParser;
import org.mule.config.spring.parsers.specific.endpoint.TransportGlobalEndpointDefinitionParser;
import org.mule.endpoint.URIBuilder;
import org.mule.transport.kafka.KafkaConnector;

/**
 * Registers a Bean Definition Parser for handling <code><kafka:connector></code> elements
 * and supporting endpoint elements.
 * 
 * @author Esteban Robles Luna <esteban.roblesluna@gmail.com>
 */
public class KafkaNamespaceHandler extends AbstractMuleNamespaceHandler
{
	public static final String TOPIC = "topic";

	public static final String[][] KAFKA_ATTRIBUTES = new String[][]{new String[]{TOPIC}};
    
    public void init()
    {
        registerConnectorDefinitionParser(KafkaConnector.class);

        registerKafkaEndpointDefinitionParser("endpoint", new TransportGlobalEndpointDefinitionParser(KafkaConnector.KAFKA, TransportGlobalEndpointDefinitionParser.PROTOCOL, TransportGlobalEndpointDefinitionParser.RESTRICTED_ENDPOINT_ATTRIBUTES, KAFKA_ATTRIBUTES, new String[][]{}));
        registerKafkaEndpointDefinitionParser("inbound-endpoint", new TransportEndpointDefinitionParser(KafkaConnector.KAFKA, TransportEndpointDefinitionParser.PROTOCOL, InboundEndpointFactoryBean.class, TransportEndpointDefinitionParser.RESTRICTED_ENDPOINT_ATTRIBUTES, KAFKA_ATTRIBUTES, new String[][]{}));
        registerKafkaEndpointDefinitionParser("outbound-endpoint", new TransportEndpointDefinitionParser(KafkaConnector.KAFKA, TransportEndpointDefinitionParser.PROTOCOL, OutboundEndpointFactoryBean.class, TransportEndpointDefinitionParser.RESTRICTED_ENDPOINT_ATTRIBUTES, KAFKA_ATTRIBUTES, new String[][]{}));
    }
    
    protected void registerKafkaEndpointDefinitionParser(String element, MuleDefinitionParser parser)
    {
        parser.addAlias(TOPIC, URIBuilder.PATH);

        registerBeanDefinitionParser(element, parser);
    }
}
