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

import org.mule.api.MuleException;
import org.mule.api.endpoint.OutboundEndpoint;
import org.mule.api.transport.MessageDispatcher;
import org.mule.transport.AbstractMessageDispatcherFactory;

/**
 * The factory for creating dispatchers for kafka.
 * 
 * @author Esteban Robles Luna <esteban.roblesluna@gmail.com>
 */
public class KafkaMessageDispatcherFactory extends AbstractMessageDispatcherFactory
{
    @Override
    public MessageDispatcher create(OutboundEndpoint endpoint) throws MuleException
    {
        return new KafkaMessageDispatcher(endpoint);
    }
}
