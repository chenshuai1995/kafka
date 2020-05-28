/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package kafka.cluster

import kafka.log.Log
import kafka.utils.{SystemTime, Time, Logging}
import kafka.server.{LogReadResult, LogOffsetMetadata}
import kafka.common.KafkaException

import java.util.concurrent.atomic.AtomicLong

class Replica(val brokerId: Int,// 副本所在的broker id
              val partition: Partition,// 副本对应的分区
              time: Time = SystemTime,
              initialHighWatermarkValue: Long = 0L,
              val log: Option[Log] = None// 本地副本对应的Log对象，远程副本此字段为空
             ) extends Logging {
  // the high watermark offset value, in non-leader replicas only its message offsets are kept
  // HW的值
  @volatile private[this] var highWatermarkMetadata: LogOffsetMetadata = new LogOffsetMetadata(initialHighWatermarkValue)
  // the log end offset value, kept in all replicas;
  // for local replica it is the log's end offset, for remote replicas its value is only updated by follower fetch
  // LEO的值
  @volatile private[this] var logEndOffsetMetadata: LogOffsetMetadata = LogOffsetMetadata.UnknownOffsetMetadata

  val topic = partition.topic
  val partitionId = partition.partitionId

  def isLocal: Boolean = {
    log match {// 创建Replica对象时，指定了Log，则表示本地副本
      case Some(l) => true
      case None => false
    }
  }

  // 记录follower副本最后一次追赶上leader的时间戳
  private[this] val lastCaughtUpTimeMsUnderlying = new AtomicLong(time.milliseconds)

  def lastCaughtUpTimeMs = lastCaughtUpTimeMsUnderlying.get()

  def updateLogReadResult(logReadResult : LogReadResult) {
    // 更新LEO
    logEndOffset = logReadResult.info.fetchOffsetMetadata

    /* If the request read up to the log end offset snapshot when the read was initiated,
     * set the lastCaughtUpTimeMsUnderlying to the current time.
     * This means that the replica is fully caught up.
     */
    // 更新lastCaughtUpTimeMsUnderlying，表示此时follower副本的同步进度完全追赶上了leader副本
    if(logReadResult.isReadFromLogEnd) {
      lastCaughtUpTimeMsUnderlying.set(time.milliseconds)
    }
  }

  private def logEndOffset_=(newLogEndOffset: LogOffsetMetadata) {
    // 对于本地副本，不能直接更新LEO，其LEO由Log.logEndOffsetMetadata字段决定
    if (isLocal) {
      throw new KafkaException("Should not set log end offset on partition [%s,%d]'s local replica %d".format(topic, partitionId, brokerId))
    } else {// 对于远程副本，LEO是通过请求进行更新的
      logEndOffsetMetadata = newLogEndOffset
      trace("Setting log end offset for replica %d for partition [%s,%d] to [%s]"
        .format(brokerId, topic, partitionId, logEndOffsetMetadata))
    }
  }

  // 获取LEO
  def logEndOffset =
    if (isLocal)
      log.get.logEndOffsetMetadata
    else
      logEndOffsetMetadata

  def highWatermark_=(newHighWatermark: LogOffsetMetadata) {
    if (isLocal) {// 只有本地副本可以更新HW
      highWatermarkMetadata = newHighWatermark
      trace("Setting high watermark for replica %d partition [%s,%d] on broker %d to [%s]"
        .format(brokerId, topic, partitionId, brokerId, newHighWatermark))
    } else {
      throw new KafkaException("Should not set high watermark on partition [%s,%d]'s non-local replica %d".format(topic, partitionId, brokerId))
    }
  }

  def highWatermark = highWatermarkMetadata

  def convertHWToLocalOffsetMetadata() = {
    if (isLocal) {
      highWatermarkMetadata = log.get.convertToOffsetMetadata(highWatermarkMetadata.messageOffset)
    } else {
      throw new KafkaException("Should not construct complete high watermark on partition [%s,%d]'s non-local replica %d".format(topic, partitionId, brokerId))
    }
  }

  override def equals(that: Any): Boolean = {
    if(!(that.isInstanceOf[Replica]))
      return false
    val other = that.asInstanceOf[Replica]
    if(topic.equals(other.topic) && brokerId == other.brokerId && partition.equals(other.partition))
      return true
    false
  }

  override def hashCode(): Int = {
    31 + topic.hashCode() + 17*brokerId + partition.hashCode()
  }


  override def toString(): String = {
    val replicaString = new StringBuilder
    replicaString.append("ReplicaId: " + brokerId)
    replicaString.append("; Topic: " + topic)
    replicaString.append("; Partition: " + partition.partitionId)
    replicaString.append("; isLocal: " + isLocal)
    if(isLocal) replicaString.append("; Highwatermark: " + highWatermark)
    replicaString.toString()
  }
}
