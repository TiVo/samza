/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.migration

import java.util
import javax.security.auth.login.Configuration

import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils._
import kafka.zk.EmbeddedZookeeper
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.security.JaasUtils
import org.apache.samza.checkpoint.Checkpoint
import org.apache.samza.checkpoint.kafka.{KafkaCheckpointLogKey, KafkaCheckpointManager, KafkaCheckpointManagerFactory}
import org.apache.samza.config._
import org.apache.samza.container.TaskName
import org.apache.samza.container.grouper.stream.GroupByPartitionFactory
import org.apache.samza.coordinator.MockSystemFactory
import org.apache.samza.coordinator.stream.messages.SetMigrationMetaMessage
import org.apache.samza.coordinator.stream._
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.serializers.CheckpointSerde
import org.apache.samza.storage.ChangelogPartitionManager
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.util._
import org.apache.samza.Partition
import org.junit.Assert._
import org.junit._

import scala.collection.JavaConversions._
import scala.collection._
import scala.collection.mutable.Buffer

class TestKafkaCheckpointMigration {

  val checkpointTopic = "checkpoint-topic"
  val serdeCheckpointTopic = "checkpoint-topic-invalid-serde"
  val checkpointTopicConfig = KafkaCheckpointManagerFactory.getCheckpointTopicProperties(null)

  val zkConnectionTimeout = 6000
  val zkSessionTimeout = 6000

  var zkUtils: ZkUtils = null
  var zookeeper: EmbeddedZookeeper = null
  var brokers: String = null
  def zkPort: Int = zookeeper.port
  def zkConnect: String = s"127.0.0.1:$zkPort"

  var producer: Producer[Array[Byte], Array[Byte]] = null

  val partition = new Partition(0)
  val partition2 = new Partition(1)
  val cp1 = new Checkpoint(Map(new SystemStreamPartition("kafka", "topic", partition) -> "123"))
  val cp2 = new Checkpoint(Map(new SystemStreamPartition("kafka", "topic", partition) -> "12345"))
  var server1: KafkaServer = null
  var server2: KafkaServer = null
  var server3: KafkaServer = null
  var metadataStore: TopicMetadataStore = null

  val systemStreamPartitionGrouperFactoryString = classOf[GroupByPartitionFactory].getCanonicalName

  var servers: Buffer[KafkaServer] = null

  @Before
  def beforeSetupServers {
    zookeeper = new EmbeddedZookeeper()
    zkUtils = ZkUtils(zkConnect, zkSessionTimeout, zkConnectionTimeout, JaasUtils.isZkSecurityEnabled())

    val props = TestUtils.createBrokerConfigs(3, zkConnect, true)

    val configs = props.map(p => {
      p.setProperty("auto.create.topics.enable","false")
      KafkaConfig.fromProps(p)
    })

    servers = configs.map(TestUtils.createServer(_)).toBuffer

    val brokerList = TestUtils.getBrokerListStrFromServers(servers, SecurityProtocol.PLAINTEXT)
    brokers = brokerList.split(",").map(p => "localhost" + p).mkString(",")

    var jobConfig = Map("systems.kafka.consumer.zookeeper.connect" -> zkConnect,
      "systems.kafka.producer.bootstrap.servers" -> brokers)

    val config = new util.HashMap[String, Object]()

    config.put("bootstrap.servers", brokers)
    config.put("request.required.acks", "-1")
    config.put("serializer.class", "kafka.serializer.StringEncoder")
    config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
    config.put(ProducerConfig.RETRIES_CONFIG, (new Integer(Integer.MAX_VALUE-1)).toString())
    val producerConfig = new KafkaProducerConfig("kafka", "i001", config)

    producer = new KafkaProducer[Array[Byte], Array[Byte]](producerConfig.getProducerProperties)
    metadataStore = new ClientUtilTopicMetadataStore(brokers, "some-job-name")
  }

  @After
  def afterCleanLogDirs {
    servers.foreach(_.shutdown())
    servers.foreach(server => CoreUtils.delete(server.config.logDirs))

    if (zkUtils != null)
      CoreUtils.swallow(zkUtils.close())
    if (zookeeper != null)
      CoreUtils.swallow(zookeeper.shutdown())
    Configuration.setConfiguration(null)
  }

  private def writeChangeLogPartitionMapping(changelogMapping: Map[TaskName, Integer], cpTopic: String = checkpointTopic) = {
    val record = new ProducerRecord[Array[Byte], Array[Byte]](
      cpTopic,
      0,
      KafkaCheckpointLogKey.getChangelogPartitionMappingKey().toBytes(),
      new CheckpointSerde().changelogPartitionMappingToBytes(changelogMapping)
    )
    try {
      producer.send(record).get()
    } catch {
      case e: Exception => println(e.getMessage)
    } finally {
      producer.close()
    }
  }

  @Test
  def testMigrationWithNoCheckpointTopic() {
    val mapConfig = Map[String, String](
      "task.checkpoint.factory" -> "org.apache.samza.checkpoint.kafka.KafkaCheckpointManagerFactory",
      JobConfig.JOB_NAME -> "test",
      JobConfig.JOB_COORDINATOR_SYSTEM -> "coordinator",
      JobConfig.JOB_CONTAINER_COUNT -> "2",
      "task.inputs" -> "test.stream1",
      "task.checkpoint.system" -> "test",
      SystemConfig.SYSTEM_FACTORY.format("test") -> classOf[MockSystemFactory].getCanonicalName,
      "systems.test.producer.bootstrap.servers" -> brokers,
      "systems.test.consumer.zookeeper.connect" -> zkConnect,
      SystemConfig.SYSTEM_FACTORY.format("coordinator") -> classOf[MockCoordinatorStreamSystemFactory].getName)

    // Enable consumer caching
    MockCoordinatorStreamSystemFactory.enableMockConsumerCache()

    val config: MapConfig = new MapConfig(mapConfig)
    val migrate = new KafkaCheckpointMigration
    migrate.migrate(config)
    val consumer = new CoordinatorStreamSystemFactory().getCoordinatorStreamSystemConsumer(config, new NoOpMetricsRegistry)
    consumer.register()
    consumer.start()
    consumer.bootstrap()
    val bootstrappedStream = consumer.getBootstrappedStream(SetMigrationMetaMessage.TYPE)
    assertEquals(1, bootstrappedStream.size())

    val expectedMigrationMessage = new SetMigrationMetaMessage("CHECKPOINTMIGRATION", "CheckpointMigration09to10", "true")
    assertEquals(expectedMigrationMessage, bootstrappedStream.head)
    consumer.stop()
  }

  @Test
  def testMigration() {
    try {
      val mapConfig = Map(
        "task.checkpoint.factory" -> "org.apache.samza.checkpoint.kafka.KafkaCheckpointManagerFactory",
        JobConfig.JOB_NAME -> "test",
        JobConfig.JOB_COORDINATOR_SYSTEM -> "coordinator",
        JobConfig.JOB_CONTAINER_COUNT -> "2",
        "task.inputs" -> "test.stream1",
        "task.checkpoint.system" -> "test",
        "systems.test.producer.bootstrap.servers" -> brokers,
        "systems.test.consumer.zookeeper.connect" -> zkConnect,
        SystemConfig.SYSTEM_FACTORY.format("test") -> classOf[MockSystemFactory].getCanonicalName,
        SystemConfig.SYSTEM_FACTORY.format("coordinator") -> classOf[MockCoordinatorStreamSystemFactory].getName)
      // Enable consumer caching
      MockCoordinatorStreamSystemFactory.enableMockConsumerCache()

      val config = new MapConfig(mapConfig)
      val checkpointTopicName = KafkaUtil.getCheckpointTopic("test", "1")
      val checkpointManager = new KafkaCheckpointManagerFactory().getCheckpointManager(config, new NoOpMetricsRegistry).asInstanceOf[KafkaCheckpointManager]

      // Write a couple of checkpoints in the old checkpoint topic
      val task1 = new TaskName(partition.toString)
      val task2 = new TaskName(partition2.toString)
      checkpointManager.start
      checkpointManager.register(task1)
      checkpointManager.register(task2)
      checkpointManager.writeCheckpoint(task1, cp1)
      checkpointManager.writeCheckpoint(task2, cp2)

      // Write changelog partition info to the old checkpoint topic
      val changelogMapping = Map(task1 -> 1.asInstanceOf[Integer], task2 -> 10.asInstanceOf[Integer])
      writeChangeLogPartitionMapping(changelogMapping, checkpointTopicName)
      checkpointManager.stop

      // Initialize coordinator stream
      val coordinatorFactory = new CoordinatorStreamSystemFactory()
      val coordinatorSystemConsumer = coordinatorFactory.getCoordinatorStreamSystemConsumer(config, new MetricsRegistryMap)
      val coordinatorSystemProducer = coordinatorFactory.getCoordinatorStreamSystemProducer(config, new MetricsRegistryMap)
      coordinatorSystemConsumer.register()
      coordinatorSystemConsumer.start()

      assertEquals(coordinatorSystemConsumer.getBootstrappedStream(SetMigrationMetaMessage.TYPE).size, 0)
      coordinatorSystemConsumer.stop

      // Start the migration
      val migrationInstance = new KafkaCheckpointMigration
      migrationInstance.migrate(config)

      // Verify if the changelogPartitionInfo has been migrated
      val newChangelogManager = new ChangelogPartitionManager(coordinatorSystemProducer, coordinatorSystemConsumer, "test")
      newChangelogManager.start
      val newChangelogMapping = newChangelogManager.readChangeLogPartitionMapping()
      newChangelogManager.stop
      assertEquals(newChangelogMapping.toMap, changelogMapping)

      // Check for migration message
      coordinatorSystemConsumer.register()
      coordinatorSystemConsumer.start()
      assertEquals(coordinatorSystemConsumer.getBootstrappedStream(SetMigrationMetaMessage.TYPE).size, 1)
      coordinatorSystemConsumer.stop()
    }
    finally {
      MockCoordinatorStreamSystemFactory.disableMockConsumerCache()
    }
  }

  class MockKafkaCheckpointMigration extends KafkaCheckpointMigration{
    var migrationCompletionMarkFlag: Boolean = false
    var migrationVerificationMarkFlag: Boolean = false

    override def migrationCompletionMark(coordinatorStreamProducer: CoordinatorStreamSystemProducer) = {
      migrationCompletionMarkFlag = true
      super.migrationCompletionMark(coordinatorStreamProducer)
    }

    override def migrationVerification(coordinatorStreamConsumer: CoordinatorStreamSystemConsumer): Boolean = {
      migrationVerificationMarkFlag = true
      super.migrationVerification(coordinatorStreamConsumer)
    }
  }
}
