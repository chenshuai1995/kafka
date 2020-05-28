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

import kafka.common._
import kafka.utils._
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.admin.AdminUtils
import kafka.api.LeaderAndIsr
import kafka.log.LogConfig
import kafka.server._
import kafka.metrics.KafkaMetricsGroup
import kafka.controller.KafkaController
import kafka.message.ByteBufferMessageSet
import java.io.IOException
import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.kafka.common.errors.{NotEnoughReplicasException, NotLeaderForPartitionException}
import org.apache.kafka.common.protocol.Errors

import scala.collection.JavaConverters._
import com.yammer.metrics.core.Gauge
import org.apache.kafka.common.requests.PartitionState

/**
 * Data structure that represents a topic partition. The leader maintains the AR, ISR, CUR, RAR
 */
class Partition(val topic: String,
                val partitionId: Int,
                time: Time,
                replicaManager: ReplicaManager) extends Logging with KafkaMetricsGroup {
  // 当前broker的id，可以和replicaId比较，从而判断指定的Replica对是否表示本地副本
  private val localBrokerId = replicaManager.config.brokerId
  // 当前broker的LogManager对象
  private val logManager = replicaManager.logManager
  private val zkUtils = replicaManager.zkUtils
  // 该分区的全部副本集合
  private val assignedReplicaMap = new Pool[Int, Replica]
  // The read lock is only required when multiple reads are executed and needs to be in a consistent manner
  private val leaderIsrUpdateLock = new ReentrantReadWriteLock()
  private var zkVersion: Int = LeaderAndIsr.initialZKVersion
  // Leader副本的年代信息
  @volatile private var leaderEpoch: Int = LeaderAndIsr.initialLeaderEpoch - 1
  // 该分区的leader副本的id
  @volatile var leaderReplicaIdOpt: Option[Int] = None
  // ISR
  @volatile var inSyncReplicas: Set[Replica] = Set.empty[Replica]

  /* Epoch of the controller that last changed the leader. This needs to be initialized correctly upon broker startup.
   * One way of doing that is through the controller's start replica state change command. When a new broker starts up
   * the controller sends it a start replica command containing the leader for each partition that the broker hosts.
   * In addition to the leader, the controller can also send the epoch of the controller that elected the leader for
   * each partition. */
  private var controllerEpoch: Int = KafkaController.InitialControllerEpoch - 1
  this.logIdent = "Partition [%s,%d] on broker %d: ".format(topic, partitionId, localBrokerId)

  private def isReplicaLocal(replicaId: Int) : Boolean = (replicaId == localBrokerId)
  val tags = Map("topic" -> topic, "partition" -> partitionId.toString)

  newGauge("UnderReplicated",
    new Gauge[Int] {
      def value = {
        if (isUnderReplicated) 1 else 0
      }
    },
    tags
  )

  def isUnderReplicated(): Boolean = {
    leaderReplicaIfLocal() match {
      case Some(_) =>
        inSyncReplicas.size < assignedReplicas.size
      case None =>
        false
    }
  }

  // 创建副本
  def getOrCreateReplica(replicaId: Int = localBrokerId): Replica = {
    // 从assignedReplicaMap中查找Replica对象
    val replicaOpt = getReplica(replicaId)
    replicaOpt match {
      case Some(replica) => replica
      case None =>
        if (isReplicaLocal(replicaId)) {// 判断是否为Local Replica
          // 获取配置信息，zookeeper中的配置会覆盖默认的配置
          val config = LogConfig.fromProps(logManager.defaultConfig.originals,
                                           AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic, topic))
          // 创建Local Replica对应的Log
          val log = logManager.createLog(TopicAndPartition(topic, partitionId), config)
          // 获取指定目录对应的OffsetCheckpoint对象，它负责管理该log目录下的
          // replication-offset-checkpoint文件。
          val checkpoint = replicaManager.highWatermarkCheckpoints(log.dir.getParentFile.getAbsolutePath)
          // 将replication-offset-checkpoint文件中记录的HW信息形成Map
          val offsetMap = checkpoint.read
          if (!offsetMap.contains(TopicAndPartition(topic, partitionId)))
            info("No checkpointed highwatermark is found for partition [%s,%d]".format(topic, partitionId))
          // 根据TopicAndPartition找到对应的HW，再与LEO比较，此值会作为此副本的HW
          val offset = offsetMap.getOrElse(TopicAndPartition(topic, partitionId), 0L).min(log.logEndOffset)
          // 创建Replica对象，并添加到assignedReplicaMap集合中管理
          val localReplica = new Replica(replicaId, this, time, offset, Some(log))
          addReplicaIfNotExists(localReplica)
        } else {
          // Remote Replica比较简单，直接创建Replica对象并添加到AR(assignedReplicaMap)集合中
          val remoteReplica = new Replica(replicaId, this, time)
          addReplicaIfNotExists(remoteReplica)
        }
        getReplica(replicaId).get// 最后返回对应的Replica
    }
  }

  def getReplica(replicaId: Int = localBrokerId): Option[Replica] = {
    val replica = assignedReplicaMap.get(replicaId)
    if (replica == null)
      None
    else
      Some(replica)
  }

  def leaderReplicaIfLocal(): Option[Replica] = {
    leaderReplicaIdOpt match {
      case Some(leaderReplicaId) =>
        if (leaderReplicaId == localBrokerId)
          getReplica(localBrokerId)
        else
          None
      case None => None
    }
  }

  def addReplicaIfNotExists(replica: Replica) = {
    assignedReplicaMap.putIfNotExists(replica.brokerId, replica)
  }

  def assignedReplicas(): Set[Replica] = {
    assignedReplicaMap.values.toSet
  }

  def removeReplica(replicaId: Int) {
    assignedReplicaMap.remove(replicaId)
  }

  // 删除对应分区在此Broker上的Log文件
  def delete() {
    // need to hold the lock to prevent appendMessagesToLeader() from hitting I/O exceptions due to log being deleted
    inWriteLock(leaderIsrUpdateLock) {
      assignedReplicaMap.clear()
      inSyncReplicas = Set.empty[Replica]
      leaderReplicaIdOpt = None
      try {
        logManager.deleteLog(TopicAndPartition(topic, partitionId))
        removePartitionMetrics()
      } catch {
        case e: IOException =>
          fatal("Error deleting the log for partition [%s,%d]".format(topic, partitionId), e)
          Runtime.getRuntime().halt(1)
      }
    }
  }

  def getLeaderEpoch(): Int = {
    return this.leaderEpoch
  }

  /**
   * Make the local replica the leader by resetting LogEndOffset for remote replicas (there could be old LogEndOffset
   * from the time when this broker was the leader last time) and setting the new leader and ISR.
   * If the leader replica id does not change, return false to indicate the replica manager.
   */
  // 将Local Replica设置成Leader副本
  def makeLeader(controllerId: Int,
                 partitionStateInfo: PartitionState,
                 correlationId: Int): Boolean = {
    val (leaderHWIncremented, isNewLeader) = inWriteLock(leaderIsrUpdateLock) {
      // 获取需要分配的AR集合
      val allReplicas = partitionStateInfo.replicas.asScala.map(_.toInt)
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      // 记录controllerEpoch
      controllerEpoch = partitionStateInfo.controllerEpoch
      // add replicas that are new
      // 创建AR集合中所有副本对应的Replica对象
      allReplicas.foreach(replica => getOrCreateReplica(replica))
      // 获取ISR集合
      val newInSyncReplicas = partitionStateInfo.isr.asScala.map(r => getOrCreateReplica(r)).toSet
      // remove assigned replicas that have been removed by the controller
      // 根据allReplicas更新assignedReplicas集合
      (assignedReplicas().map(_.brokerId) -- allReplicas).foreach(removeReplica(_))
      // 更新Partition字段
      inSyncReplicas = newInSyncReplicas// 更新ISR集合
      leaderEpoch = partitionStateInfo.leaderEpoch// 更新leaderEpoch
      zkVersion = partitionStateInfo.zkVersion// 更新zkVersion
      // 检测Leader是否发生变化
      val isNewLeader =
        if (leaderReplicaIdOpt.isDefined && leaderReplicaIdOpt.get == localBrokerId) {
          false// Leader所在的Broker并没有发生变化，则为false
        } else {
          // Leader之前并不在此Broker上，即Leader发生变化，更新leaderReplicaIdOpt，则为true
          leaderReplicaIdOpt = Some(localBrokerId)
          true
        }
      val leaderReplica = getReplica().get// 获取Local Replica
      // we may need to increment high watermark since ISR could be down to 1
      if (isNewLeader) {
        // construct the high watermark metadata for the new leader replica
        // 初始化Leader的hightWatermarkMetadata
        leaderReplica.convertHWToLocalOffsetMetadata()
        // reset log end offset for remote replicas
        // 重置所有Remote Replica的LEO值为-1
        assignedReplicas.filter(_.brokerId != localBrokerId).foreach(_.updateLogReadResult(LogReadResult.UnknownLogReadResult))
      }
      // 尝试更新HW
      (maybeIncrementLeaderHW(leaderReplica), isNewLeader)
    }
    // some delayed operations may be unblocked after HW changed
    // 如果Leader副本的HW增加了，则可能有DelayedFetch满足执行条件，所以这里调用
    // tryCompleteDelayedRequests()方法会检查delayedProducePurgatory、
    // delayedFetchPurgatory中key为当前分区的延时操作
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()
    isNewLeader
  }

  /**
   *  Make the local replica the follower by setting the new leader and ISR to empty
   *  If the leader replica id does not change, return false to indicate the replica manager
   */
  def makeFollower(controllerId: Int, partitionStateInfo: PartitionState, correlationId: Int): Boolean = {
    inWriteLock(leaderIsrUpdateLock) {
      val allReplicas = partitionStateInfo.replicas.asScala.map(_.toInt)
      val newLeaderBrokerId: Int = partitionStateInfo.leader
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      controllerEpoch = partitionStateInfo.controllerEpoch
      // add replicas that are new
      // 创建对应的Replica对象
      allReplicas.foreach(r => getOrCreateReplica(r))
      // remove assigned replicas that have been removed by the controller
      // 与makeLeader()一样，根据partitionStateInfo信息更新AR集合
      (assignedReplicas().map(_.brokerId) -- allReplicas).foreach(removeReplica(_))
      // 空集合，ISR集合在Leader副本上进行维护，在Follower副本上不维护ISR集合信息
      inSyncReplicas = Set.empty[Replica]
      leaderEpoch = partitionStateInfo.leaderEpoch// 更新leaderEpoch
      zkVersion = partitionStateInfo.zkVersion// 更新zkVersion字段

      // 检测Leader是否发生变化
      if (leaderReplicaIdOpt.isDefined && leaderReplicaIdOpt.get == newLeaderBrokerId) {
        false
      }
      else {
        // 更新leaderReplicaIdOpt
        leaderReplicaIdOpt = Some(newLeaderBrokerId)
        true
      }
    }
  }

  /**
   * Update the log end offset of a certain replica of this partition
   */
  def updateReplicaLogReadResult(replicaId: Int, logReadResult: LogReadResult) {
    getReplica(replicaId) match {
      case Some(replica) =>
        // 更新Follower副本的状态
        replica.updateLogReadResult(logReadResult)
        // check if we need to expand ISR to include this replica
        // if it is not in the ISR yet
        // 检测ISR是否扩张，并与zookeeper进行同步
        maybeExpandIsr(replicaId)

        debug("Recorded replica %d log end offset (LEO) position %d for partition %s."
          .format(replicaId,
                  logReadResult.info.fetchOffsetMetadata.messageOffset,
                  TopicAndPartition(topic, partitionId)))
      case None =>
        throw new NotAssignedReplicaException(("Leader %d failed to record follower %d's position %d since the replica" +
          " is not recognized to be one of the assigned replicas %s for partition %s.")
          .format(localBrokerId,
                  replicaId,
                  logReadResult.info.fetchOffsetMetadata.messageOffset,
                  assignedReplicas().map(_.brokerId).mkString(","),
                  TopicAndPartition(topic, partitionId)))
    }
  }

  /**
   * Check and maybe expand the ISR of the partition.
   *
   * This function can be triggered when a replica's LEO has incremented
   */
  // ISR集合扩张
  def maybeExpandIsr(replicaId: Int) {
    val leaderHWIncremented = inWriteLock(leaderIsrUpdateLock) {
      // check if this replica needs to be added to the ISR
      // 只有Leader副本才会管理ISR，所以先获取Leader副本对应的Replica对象
      leaderReplicaIfLocal() match {
        case Some(leaderReplica) =>
          val replica = getReplica(replicaId).get
          // 获取当前HW
          val leaderHW = leaderReplica.highWatermark
          // 下面3个检测条件依次是：
          // Follower副本不在ISR集合中，
          // AR集合中可以查找到Follower副本，
          // Follower副本的LEO已经追赶上HW
          if(!inSyncReplicas.contains(replica) &&
             assignedReplicas.map(_.brokerId).contains(replicaId) &&
                  replica.logEndOffset.offsetDiff(leaderHW) >= 0) {
            // 将新ISR集合的信息上传到zookeeper中保存
            // 并更新Partition.inSyncReplicas字段
            val newInSyncReplicas = inSyncReplicas + replica
            info("Expanding ISR for partition [%s,%d] from %s to %s"
                         .format(topic, partitionId, inSyncReplicas.map(_.brokerId).mkString(","),
                                 newInSyncReplicas.map(_.brokerId).mkString(",")))
            // update ISR in ZK and cache
            updateIsr(newInSyncReplicas)
            replicaManager.isrExpandRate.mark()
          }

          // check if the HW of the partition can now be incremented
          // since the replica maybe now be in the ISR and its LEO has just incremented
          // 尝试更新HW
          maybeIncrementLeaderHW(leaderReplica)

        case None => false // nothing to do if no longer leader
      }
    }

    // some delayed operations may be unblocked after HW changed
    // 尝试执行延时任务
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()
  }

  /*
   * Note that this method will only be called if requiredAcks = -1
   * and we are waiting for all replicas in ISR to be fully caught up to
   * the (local) leader's offset corresponding to this produce request
   * before we acknowledge the produce request.
   */
  // 检测其参数指定的消息是否已经被ISR集合中所有Follower副本同步
  def checkEnoughReplicasReachOffset(requiredOffset: Long): (Boolean, Short) = {
    leaderReplicaIfLocal() match {// 获取Leader副本对应的Replica
      case Some(leaderReplica) =>
        // keep the current immutable replica list reference
        // 获取当前ISR集合
        val curInSyncReplicas = inSyncReplicas
        val numAcks = curInSyncReplicas.count(r => {
          if (!r.isLocal)
            if (r.logEndOffset.messageOffset >= requiredOffset) {
              trace("Replica %d of %s-%d received offset %d".format(r.brokerId, topic, partitionId, requiredOffset))
              true
            }
            else
              false
          else
            true /* also count the local (leader) replica */
        })

        trace("%d acks satisfied for %s-%d with acks = -1".format(numAcks, topic, partitionId))

        val minIsr = leaderReplica.log.get.config.minInSyncReplicas

        // 比较HW与消息的offset
        if (leaderReplica.highWatermark.messageOffset >= requiredOffset ) {
          /*
          * The topic may be configured not to accept messages if there are not enough replicas in ISR
          * in this scenario the request was already appended locally and then added to the purgatory before the ISR was shrunk
          */
          if (minIsr <= curInSyncReplicas.size) {// 检测ISR集合大小是否合法
            (true, Errors.NONE.code)
          } else {
            // ISR集合太小，返回相应的错误码
            (true, Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND.code)
          }
        } else
          (false, Errors.NONE.code)
      case None =>
        (false, Errors.NOT_LEADER_FOR_PARTITION.code)
    }
  }

  /**
   * Check and maybe increment the high watermark of the partition;
   * this function can be triggered when
   *
   * 1. Partition ISR changed
   * 2. Any replica's LEO changed
   *
   * Returns true if the HW was incremented, and false otherwise.
   * Note There is no need to acquire the leaderIsrUpdate lock here
   * since all callers of this private API acquire that lock
   */
  private def maybeIncrementLeaderHW(leaderReplica: Replica): Boolean = {
    // 获取ISR集合中所有副本的LEO
    val allLogEndOffsets = inSyncReplicas.map(_.logEndOffset)
    // 将ISR集合中最小的LEO作为新HW
    val newHighWatermark = allLogEndOffsets.min(new LogOffsetMetadata.OffsetOrdering)
    val oldHighWatermark = leaderReplica.highWatermark// 获取当前HW
    // 比较新旧两个HW的值，决定是否更新HW
    if (oldHighWatermark.messageOffset < newHighWatermark.messageOffset || oldHighWatermark.onOlderSegment(newHighWatermark)) {
      leaderReplica.highWatermark = newHighWatermark// 更新HW
      debug("High watermark for partition [%s,%d] updated to %s".format(topic, partitionId, newHighWatermark))
      true
    } else {
      debug("Skipping update high watermark since Old hw %s is larger than new hw %s for partition [%s,%d]. All leo's are %s"
        .format(oldHighWatermark, newHighWatermark, topic, partitionId, allLogEndOffsets.mkString(",")))
      false
    }
  }

  /**
   * Try to complete any pending requests. This should be called without holding the leaderIsrUpdateLock.
   */
  private def tryCompleteDelayedRequests() {
    val requestKey = new TopicPartitionOperationKey(this.topic, this.partitionId)
    replicaManager.tryCompleteDelayedFetch(requestKey)
    replicaManager.tryCompleteDelayedProduce(requestKey)
  }

  // ISR集合缩减
  def maybeShrinkIsr(replicaMaxLagTimeMs: Long) {
    val leaderHWIncremented = inWriteLock(leaderIsrUpdateLock) {
      // 调用leaderReplicaIfLocal()方法获取Leader副本对应的Replica对象
      leaderReplicaIfLocal() match {
        case Some(leaderReplica) =>
          // 通过检测Follower副本的lastCaughtUpTimeMsUnderlying字段，找出
          // 已滞后的Follower副本集合，该滞后集合中的Follower副本会被剔除了ISR集合
          // 无论是长时间没有与Leader副本进行同步还是其LEO与HW相差太大，都可以从此字段反映出来
          val outOfSyncReplicas = getOutOfSyncReplicas(leaderReplica, replicaMaxLagTimeMs)
          if(outOfSyncReplicas.size > 0) {
            // 将outOfSyncReplicas从ISR集合中删除，生成新的ISR集合
            val newInSyncReplicas = inSyncReplicas -- outOfSyncReplicas
            assert(newInSyncReplicas.size > 0)
            info("Shrinking ISR for partition [%s,%d] from %s to %s".format(topic, partitionId,
              inSyncReplicas.map(_.brokerId).mkString(","), newInSyncReplicas.map(_.brokerId).mkString(",")))
            // update ISR in zk and in cache
            // 将新ISR集合的信息上传到zookeeper中保存，并更新inSyncReplica字段
            updateIsr(newInSyncReplicas)
            // we may need to increment high watermark since ISR could be down to 1

            replicaManager.isrShrinkRate.mark()
            maybeIncrementLeaderHW(leaderReplica)// 更新Leader的HW
          } else {
            false
          }

        case None => false // do nothing if no longer leader
      }
    }

    // some delayed operations may be unblocked after HW changed
    if (leaderHWIncremented)// 尝试执行延时任务
      tryCompleteDelayedRequests()
  }

  def getOutOfSyncReplicas(leaderReplica: Replica, maxLagMs: Long): Set[Replica] = {
    /**
     * there are two cases that will be handled here -
     * 1. Stuck followers: If the leo of the replica hasn't been updated for maxLagMs ms,
     *                     the follower is stuck and should be removed from the ISR
     * 2. Slow followers: If the replica has not read up to the leo within the last maxLagMs ms,
     *                    then the follower is lagging and should be removed from the ISR
     * Both these cases are handled by checking the lastCaughtUpTimeMs which represents
     * the last time when the replica was fully caught up. If either of the above conditions
     * is violated, that replica is considered to be out of sync
     *
     **/
    val leaderLogEndOffset = leaderReplica.logEndOffset
    val candidateReplicas = inSyncReplicas - leaderReplica

    val laggingReplicas = candidateReplicas.filter(r => (time.milliseconds - r.lastCaughtUpTimeMs) > maxLagMs)
    if(laggingReplicas.size > 0)
      debug("Lagging replicas for partition %s are %s".format(TopicAndPartition(topic, partitionId), laggingReplicas.map(_.brokerId).mkString(",")))

    laggingReplicas
  }

  // 向Leader副本对应的Log中追加消息的功能
  def appendMessagesToLeader(messages: ByteBufferMessageSet, requiredAcks: Int = 0) = {
    val (info, leaderHWIncremented) = inReadLock(leaderIsrUpdateLock) {
      // 获取Leader副本对应的Replica对象
      val leaderReplicaOpt = leaderReplicaIfLocal()
      leaderReplicaOpt match {
        case Some(leaderReplica) =>
          val log = leaderReplica.log.get
          // 获取配置指定的最小ISR集合大小的限制
          val minIsr = log.config.minInSyncReplicas
          val inSyncSize = inSyncReplicas.size

          // Avoid writing to leader if there are not enough insync replicas to make it safe
          // 如果当前ISR集合中副本的数量小于配置的最小限制，且生产者要求较高的可用性，
          // 则不能追加消息，生产者将收到一个NotEnoughReplicasException异常
          if (inSyncSize < minIsr && requiredAcks == -1) {
            throw new NotEnoughReplicasException("Number of insync replicas for partition [%s,%d] is [%d], below required minimum [%d]"
              .format(topic, partitionId, inSyncSize, minIsr))
          }

          // 写入Leader副本对应的Log
          val info = log.append(messages, assignOffsets = true)
          // probably unblock some follower fetch requests since log end offset has been updated
          // 尝试执行对应的DelayedFetch
          replicaManager.tryCompleteDelayedFetch(new TopicPartitionOperationKey(this.topic, this.partitionId))
          // we may need to increment high watermark since ISR could be down to 1
          // 尝试增加Leader的HW
          (info, maybeIncrementLeaderHW(leaderReplica))

        case None =>
          throw new NotLeaderForPartitionException("Leader not local for partition [%s,%d] on broker %d"
            .format(topic, partitionId, localBrokerId))
      }
    }

    // some delayed operations may be unblocked after HW changed
    if (leaderHWIncremented)// 尝试执行延迟任务
      tryCompleteDelayedRequests()

    info
  }

  private def updateIsr(newIsr: Set[Replica]) {
    val newLeaderAndIsr = new LeaderAndIsr(localBrokerId, leaderEpoch, newIsr.map(r => r.brokerId).toList, zkVersion)
    val (updateSucceeded,newVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, topic, partitionId,
      newLeaderAndIsr, controllerEpoch, zkVersion)

    if(updateSucceeded) {
      replicaManager.recordIsrChange(new TopicAndPartition(topic, partitionId))
      inSyncReplicas = newIsr
      zkVersion = newVersion
      trace("ISR updated to [%s] and zkVersion updated to [%d]".format(newIsr.mkString(","), zkVersion))
    } else {
      info("Cached zkVersion [%d] not equal to that in zookeeper, skip updating ISR".format(zkVersion))
    }
  }

  /**
   * remove deleted log metrics
   */
  private def removePartitionMetrics() {
    removeMetric("UnderReplicated", tags)
  }

  override def equals(that: Any): Boolean = {
    if(!(that.isInstanceOf[Partition]))
      return false
    val other = that.asInstanceOf[Partition]
    if(topic.equals(other.topic) && partitionId == other.partitionId)
      return true
    false
  }

  override def hashCode(): Int = {
    31 + topic.hashCode() + 17*partitionId
  }

  override def toString(): String = {
    val partitionString = new StringBuilder
    partitionString.append("Topic: " + topic)
    partitionString.append("; Partition: " + partitionId)
    partitionString.append("; Leader: " + leaderReplicaIdOpt)
    partitionString.append("; AssignedReplicas: " + assignedReplicaMap.keys.mkString(","))
    partitionString.append("; InSyncReplicas: " + inSyncReplicas.map(_.brokerId).mkString(","))
    partitionString.toString()
  }
}
