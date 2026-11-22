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

import kafka.server.KafkaConfig
import org.apache.kafka.clients.{ClientResponse, KafkaClient, RequestCompletionHandler}
import org.apache.kafka.common.Node
import org.apache.kafka.common.message.TxnCompleteRequestData
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.requests.{TransactionResult, TxnCompleteRequest}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.internals.LogContext
import org.apache.kafka.coordinator.share.ShareCoordinatorShard.TransactionCompletion
import org.apache.kafka.metadata.MetadataCache
import org.apache.kafka.server.InterBrokerNetworkClientFactory
import org.apache.kafka.server.util.{InterBrokerSendThread, RequestAndCompletionHandler}

import java.util
import java.util.concurrent.{ConcurrentLinkedQueue, Semaphore}
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable
import scala.jdk.CollectionConverters._

object ShareTransactionCompletionSender {
  def apply(config: KafkaConfig, metrics: Metrics, metadataCache: MetadataCache, time: Time): ShareTransactionCompletionSender = {
    val client = InterBrokerNetworkClientFactory.create("share-txn-complete", config, metrics, time,
      new LogContext(s"[ShareTxnCompleteSender broker=${config.brokerId}] "))
    new ShareTransactionCompletionSender(config.brokerId, config.requestTimeoutMs, config.interBrokerListenerName,
      metadataCache, client, time)
  }
}

class ShareTransactionCompletionSender(
  brokerId: Int,
  requestTimeoutMs: Int,
  listenerName: ListenerName,
  metadataCache: MetadataCache,
  client: KafkaClient,
  time: Time,
  maxPendingHints: Int = 1024
) extends InterBrokerSendThread("ShareTxnCompleteSender-" + brokerId, client, requestTimeoutMs, time) {
  require(maxPendingHints > 0)

  private val permits = new Semaphore(maxPendingHints)
  private val queued = new ConcurrentLinkedQueue[Hint]()
  private val accepting = new AtomicBoolean(true)

  private case class Hint(destination: Node, transaction: TxnCompleteRequestData.TxnCompleteTransaction) {
    private val released = new AtomicBoolean(false)
    def release(): Unit = if (released.compareAndSet(false, true)) permits.release()
  }

  def enqueue(completion: TransactionCompletion, result: TransactionResult): Unit = synchronized {
    if (!accepting.get()) return
    val names = metadataCache.topicIdsToNames()
    val destinations = mutable.HashSet.empty[Int]
    completion.sharePartitions().asScala.iterator.take(maxPendingHints).foreach { key =>
      val name = names.get(key.topicId())
      if (name != null) {
        val leader = metadataCache.getPartitionLeaderEndpoint(name, key.partition(), listenerName).orElse(null)
        if (leader != null && leader != Node.noNode() && destinations.add(leader.id()) && permits.tryAcquire()) {
          val transaction = new TxnCompleteRequestData.TxnCompleteTransaction()
            .setTransactionalId(completion.transactionalId()).setProducerId(completion.producerId())
            .setProducerEpoch(completion.producerEpoch()).setTransactionResult(result == TransactionResult.COMMIT)
          queued.add(Hint(leader, transaction))
        }
      }
    }
    wakeup()
  }

  override def generateRequests(): util.Collection[RequestAndCompletionHandler] = {
    val batches = mutable.LinkedHashMap.empty[Node, mutable.ArrayBuffer[Hint]]
    var next = queued.poll()
    while (next != null) {
      if (accepting.get()) batches.getOrElseUpdate(next.destination, mutable.ArrayBuffer.empty) += next
      else next.release()
      next = queued.poll()
    }
    val requests = new util.ArrayList[RequestAndCompletionHandler]()
    batches.foreach { case (destination, hints) =>
      hints.grouped(128).foreach { batch =>
        val data = new TxnCompleteRequestData().setTransactions(batch.map(_.transaction).asJava)
        requests.add(new RequestAndCompletionHandler(time.milliseconds(), destination,
          new TxnCompleteRequest.Builder(data, true), new RequestCompletionHandler {
            override def onComplete(response: ClientResponse): Unit = batch.foreach(_.release())
          }))
      }
    }
    requests
  }

  override def shutdown(): Unit = {
    synchronized {
      accepting.set(false)
      var next = queued.poll()
      while (next != null) {
        next.release()
        next = queued.poll()
      }
    }
    super.shutdown()
  }
}
