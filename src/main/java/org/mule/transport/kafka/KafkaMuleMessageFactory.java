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

import org.mule.transport.AbstractMuleMessageFactory;

/**
 * The factory for creating kafka messages.
 * 
 * @author Esteban Robles Luna <esteban.roblesluna@gmail.com>
 */
public class KafkaMuleMessageFactory extends AbstractMuleMessageFactory
{
    public KafkaMuleMessageFactory() {
    }
    
    @Override
    protected Class<?>[] getSupportedTransportMessageTypes()
    {
    	return new Class<?>[] {
    			String.class
    	};
    }

    @Override
    protected Object extractPayload(Object transportMessage, String encoding) throws Exception
    {
        return transportMessage;
    }
}
