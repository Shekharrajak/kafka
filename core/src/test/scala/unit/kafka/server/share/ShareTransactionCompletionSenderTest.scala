/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server.share

import org.apache.kafka.clients.KafkaClient
import org.apache.kafka.common.{Node, Uuid}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.requests.{TransactionResult, TxnCompleteRequest}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.coordinator.share.ShareCoordinatorShard.TransactionCompletion
import org.apache.kafka.metadata.MetadataCache
import org.apache.kafka.server.share.SharePartitionKey
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.Mockito.{mock, when}

import java.util
import scala.jdk.CollectionConverters._

class ShareTransactionCompletionSenderTest {
  private val metadataCache = mock(classOf[MetadataCache])
  private val networkClient = mock(classOf[KafkaClient])
  private val listener = new ListenerName("PLAINTEXT")
  private val topicId = Uuid.randomUuid()
  private val leader = new Node(1, "host", 9092)

  private def sender(capacity: Int = 1024): ShareTransactionCompletionSender = {
    when(metadataCache.topicIdsToNames()).thenReturn(util.Map.of(topicId, "input"))
    when(metadataCache.getPartitionLeaderEndpoint("input", 0, listener)).thenReturn(util.Optional.of(leader))
    when(metadataCache.getPartitionLeaderEndpoint("input", 1, listener)).thenReturn(util.Optional.of(leader))
    new ShareTransactionCompletionSender(0, 1000, listener, metadataCache, networkClient, Time.SYSTEM, capacity)
  }

  private def completion(id: String, epoch: Short = 2): TransactionCompletion =
    new TransactionCompletion(id, 17L, epoch, util.Set.of(
      SharePartitionKey.getInstance("group", topicId, 0), SharePartitionKey.getInstance("group", topicId, 1)))

  @Test
  def testBatchesByDestinationAndPreservesStagedEpoch(): Unit = {
    val channel = sender()
    channel.enqueue(completion("txn1"), TransactionResult.COMMIT)
    channel.enqueue(completion("txn2", 4), TransactionResult.ABORT)
    val requests = channel.generateRequests().asScala.toList
    assertEquals(1, requests.size)
    assertEquals(leader, requests.head.destination)
    val transactions = requests.head.request.build().asInstanceOf[TxnCompleteRequest].data().transactions().asScala
    assertEquals(Seq("txn1", "txn2"), transactions.map(_.transactionalId()).toSeq)
    assertEquals(Seq[Short](2, 4), transactions.map(_.producerEpoch()).toSeq)
    assertEquals(Seq(true, false), transactions.map(_.transactionResult()).toSeq)
    assertTrue(channel.generateRequests().isEmpty)
  }

  @Test
  def testCapacityIncludesInflightAndIsReleasedOnFailure(): Unit = {
    val channel = sender(1)
    channel.enqueue(completion("txn1"), TransactionResult.COMMIT)
    val request = channel.generateRequests().iterator().next()
    channel.enqueue(completion("txn2"), TransactionResult.ABORT)
    assertTrue(channel.generateRequests().isEmpty)
    request.handler.onComplete(null)
    request.handler.onComplete(null)
    channel.enqueue(completion("txn2"), TransactionResult.ABORT)
    channel.enqueue(completion("txn3"), TransactionResult.COMMIT)
    val requests = channel.generateRequests().asScala.toList
    assertEquals(1, requests.size)
    val transactions = requests.head.request.build().asInstanceOf[TxnCompleteRequest].data().transactions()
    assertEquals(1, transactions.size())
    assertEquals("txn2", transactions.get(0).transactionalId())
  }

  @Test
  def testSkipsMissingAndUnavailableLeaders(): Unit = {
    val channel = sender()
    when(metadataCache.getPartitionLeaderEndpoint("input", 0, listener)).thenReturn(util.Optional.empty())
    when(metadataCache.getPartitionLeaderEndpoint("input", 1, listener)).thenReturn(util.Optional.of(Node.noNode()))
    channel.enqueue(completion("txn"), TransactionResult.COMMIT)
    assertTrue(channel.generateRequests().isEmpty)
    when(metadataCache.topicIdsToNames()).thenReturn(util.Map.of[Uuid, String]())
    channel.enqueue(completion("txn"), TransactionResult.COMMIT)
    assertTrue(channel.generateRequests().isEmpty)
  }
}
