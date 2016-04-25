package org.mule.transport.kafka.integration;

import org.junit.Assert;
import org.junit.Test;
import org.mule.DefaultMuleMessage;
import org.mule.api.MuleMessage;
import org.mule.api.client.LocalMuleClient;
import org.mule.tck.junit4.FunctionalTestCase;

/**
 * An integration test case for Kafka writing and reading
 * 
 * @author Esteban Robles Luna <esteban.roblesluna@gmail.com>
 */
public class KafkaIntegrationTestCase extends FunctionalTestCase {
	
	@Override
	protected String getConfigResources() {
		return "kafka-integration-config.xml";
	}

	@Test
	public void testIntegration() throws Exception {
		//initialization delay in tests only
		Thread.sleep(1000 * 6);

		String payload = "my payload ";

		LocalMuleClient client = muleContext.getClient();
		for (int i = 0; i < 100; i++) {
			client.dispatch("vm://in", new DefaultMuleMessage(payload + i, muleContext));
		}
		
		Thread.sleep(1000 * 3);
		
		for (int i = 0; i < 100; i++) {
			MuleMessage message = client.request("vm://out", RECEIVE_TIMEOUT);
			Assert.assertNotNull(message);
			System.out.println("Receiving: " + message.getPayloadAsString());
		}
	}
}
