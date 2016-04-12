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
import org.mule.api.endpoint.EndpointURI;
import org.mule.endpoint.MuleEndpointURI;
import org.mule.tck.junit4.AbstractMuleTestCase;

/**
 * A test for kafka endpoints
 * 
 * @author Esteban Robles Luna <esteban.roblesluna@gmail.com>
 */
public class KafkaEndpointTestCase extends AbstractMuleTestCase
{
    @Test
    public void validEndpointURI() throws Exception
    {
        EndpointURI url = new MuleEndpointURI("kafka://topicA", null);
        Assert.assertEquals("kafka", url.getScheme());
        Assert.assertEquals("kafka://topicA", url.getUri().toString());
        Assert.assertEquals("topicA", url.getHost());
    }
}
