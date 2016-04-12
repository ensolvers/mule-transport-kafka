## Introduction

This module helps you use [Apache Kafka](http://kafka.apache.org/) in your Mule applications. 
To use the module, please download the jar or include it using maven.

## Use

Inside your mule configuration files use include the following XML namespaces:

```
<mule xmlns="http://www.mulesoft.org/schema/mule/core"
       xmlns:kafka="http://www.mulesoft.org/schema/mule/kafka"
...
    xsi:schemaLocation="
       http://www.mulesoft.org/schema/mule/kafka http://www.mulesoft.org/schema/mule/kafka/3.7/mule-kafka.xsd
       ">
...
```
Then you can have inbound and outbound endpoints. 
For example the following flows reads from the VM transport and writes it in Kafka (Flow: testFlow).
Then the messages are consumed from the kafka topic and send to the out VM queue (Flow: sendMsg).

```
    <flow name="testFlow">
        <vm:inbound-endpoint path="in" exchange-pattern="one-way" />
        <kafka:outbound-endpoint topic="test-kafka-topic" />
    </flow>

    <flow name="sendMsg">
        <kafka:inbound-endpoint 
            topic="test-kafka-topic" 
            groupId="test-my-group" 
            enableAutoCommit="true" 
            autoCommitIntervalMS="2000" />
        <vm:outbound-endpoint path="out" />
    </flow>
```
