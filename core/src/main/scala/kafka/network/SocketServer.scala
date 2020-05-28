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

package kafka.network

import java.io.IOException
import java.net._
import java.nio.channels._
import java.nio.channels.{Selector => NSelector}
import java.util
import java.util.concurrent._
import java.util.concurrent.atomic._

import com.yammer.metrics.core.Gauge
import kafka.cluster.{BrokerEndPoint, EndPoint}
import kafka.common.KafkaException
import kafka.metrics.KafkaMetricsGroup
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.network.{ChannelBuilders, KafkaChannel, LoginType, Mode, Selector => KSelector}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.protocol.types.SchemaException
import org.apache.kafka.common.utils.{Time, Utils}

import scala.collection._
import JavaConverters._
import scala.util.control.{ControlThrowable, NonFatal}

/**
 * An NIO socket server. The threading model is
 *   1 Acceptor thread that handles new connections
 *   Acceptor has N Processor threads that each have their own selector and read requests from sockets
 *   M Handler threads that handle requests and produce responses back to the processor threads for writing.
 */
class SocketServer(val config: KafkaConfig, val metrics: Metrics, val time: Time) extends Logging with KafkaMetricsGroup {

  // endpoints里封装了需要监听的host和port（服务器有多块网卡）。每个endpoint都会创建一个对应的Acceptor对象
  private val endpoints = config.listeners
  // processor线程个数，由num.network.threads配置，默认3个
  private val numProcessorThreads = config.numNetworkThreads
  // RequestChannel中RequestQueue中缓存的最大请求个数，默认500个
  private val maxQueuedRequests = config.queuedMaxRequests
  private val totalProcessorThreads = numProcessorThreads * endpoints.size

  // 每个IP最大能建立的连接数，默认Int.MaxValue
  private val maxConnectionsPerIp = config.maxConnectionsPerIp
  private val maxConnectionsPerIpOverrides = config.maxConnectionsPerIpOverrides

  this.logIdent = "[Socket Server on Broker " + config.brokerId + "], "

  // processor线程和handler线程之间交换数据的队列
  val requestChannel = new RequestChannel(totalProcessorThreads, maxQueuedRequests)
  // processors集合，所有endpoint对应的processor线程
  private val processors = new Array[Processor](totalProcessorThreads)

  // acceptors集合，一个endpoint对应一个acceptor
  private[network] val acceptors = mutable.Map[EndPoint, Acceptor]()
  private var connectionQuotas: ConnectionQuotas = _

  private val allMetricNames = (0 until totalProcessorThreads).map { i =>
    val tags = new util.HashMap[String, String]()
    tags.put("networkProcessor", i.toString)
    metrics.metricName("io-wait-ratio", "socket-server-metrics", tags)
  }

  /**
   * Start the socket server
   */
  def startup() {
    this.synchronized {

      connectionQuotas = new ConnectionQuotas(maxConnectionsPerIp, maxConnectionsPerIpOverrides)

      // socket的send和receive buffer缓冲区大小
      val sendBufferSize = config.socketSendBufferBytes
      val recvBufferSize = config.socketReceiveBufferBytes
      val brokerId = config.brokerId

      var processorBeginIndex = 0
      // endpoints是配置文件中的listeners=PLAINTEXT://:9092，就配置了一个
      endpoints.values.foreach { endpoint =>
        val protocol = endpoint.protocolType// PLAINTEXT协议
        // processor是配置文件中的num.network.threads=3线程数
        val processorEndIndex = processorBeginIndex + numProcessorThreads

        for (i <- processorBeginIndex until processorEndIndex)
          // 创建3个process线程实例，用于处理网络请求
          processors(i) = newProcessor(i, connectionQuotas, protocol)

        // 在初始化Acceptor线程时，
        // 会在内部初始化3个Processor线程
        // 并初始化好Selector和ServerSocketChannel（默认监听9092端口）
        val acceptor = new Acceptor(endpoint, sendBufferSize, recvBufferSize, brokerId,
          processors.slice(processorBeginIndex, processorEndIndex), connectionQuotas)
        acceptors.put(endpoint, acceptor)
        // 创建1个acceptor线程，负责nioSelector.select，监听客户端连接
        Utils.newThread("kafka-socket-acceptor-%s-%d".format(protocol.toString, endpoint.port), acceptor, false).start()
        acceptor.awaitStartup()// 主线程阻塞等待acceptor线程启动完成

        processorBeginIndex = processorEndIndex
      }
    }

    newGauge("NetworkProcessorAvgIdlePercent",
      new Gauge[Double] {
        def value = allMetricNames.map( metricName =>
          metrics.metrics().get(metricName).value()).sum / totalProcessorThreads
      }
    )

    info("Started " + acceptors.size + " acceptor threads")
  }

  // register the processor threads for notification of responses
  requestChannel.addResponseListener(id => processors(id).wakeup())

  /**
   * Shutdown the socket server
   */
  def shutdown() = {
    info("Shutting down")
    this.synchronized {
      acceptors.values.foreach(_.shutdown)// 调动所有acceptor的shutdown
      processors.foreach(_.shutdown)// 调用所有processor的shutdown
    }
    info("Shutdown completed")
  }

  def boundPort(protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): Int = {
    try {
      acceptors(endpoints(protocol)).serverChannel.socket().getLocalPort
    } catch {
      case e: Exception => throw new KafkaException("Tried to check server's port before server was started or checked for port of non-existing protocol", e)
    }
  }

  /* `protected` for test usage */
  protected[network] def newProcessor(id: Int, connectionQuotas: ConnectionQuotas, protocol: SecurityProtocol): Processor = {
    new Processor(id,
      time,
      config.socketRequestMaxBytes,
      requestChannel,
      connectionQuotas,
      config.connectionsMaxIdleMs,
      protocol,
      config.values,
      metrics
    )
  }

  /* For test usage */
  private[network] def connectionCount(address: InetAddress): Int =
    Option(connectionQuotas).fold(0)(_.get(address))

  /* For test usage */
  private[network] def processor(index: Int): Processor = processors(index)

}

/**
 * A base class with some helper variables and methods
 */
private[kafka] abstract class AbstractServerThread(connectionQuotas: ConnectionQuotas) extends Runnable with Logging {

  // 当前线程startup是否完成
  private val startupLatch = new CountDownLatch(1)
  // 当前线程shutdown是否完成
  private val shutdownLatch = new CountDownLatch(1)
  // 当前线程是否存活
  private val alive = new AtomicBoolean(true)

  def wakeup()

  /**
   * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
   */
  def shutdown(): Unit = {
    alive.set(false)
    // 唤醒当前AbstractServerThread
    wakeup()
    // 阻塞等待操作完成
    shutdownLatch.await()
  }

  /**
   * Wait for the thread to completely start up
   */
  def awaitStartup(): Unit = startupLatch.await

  /**
   * Record that the thread startup is complete
   */
  protected def startupComplete() = {
    // 唤醒阻塞的线程
    startupLatch.countDown()
  }

  /**
   * Record that the thread shutdown is complete
   */
  // 唤醒阻塞的线程
  protected def shutdownComplete() = shutdownLatch.countDown()

  /**
   * Is the server still running?
   */
  protected def isRunning = alive.get

  /**
   * Close the connection identified by `connectionId` and decrement the connection count.
   */
  def close(selector: KSelector, connectionId: String) {
    val channel = selector.channel(connectionId)
    if (channel != null) {
      debug(s"Closing selector connection $connectionId")
      val address = channel.socketAddress
      if (address != null)
        connectionQuotas.dec(address)
      selector.close(connectionId)
    }
  }

  /**
   * Close `channel` and decrement the connection count.
   */
  def close(channel: SocketChannel) {
    if (channel != null) {
      debug("Closing connection from " + channel.socket.getRemoteSocketAddress())
      connectionQuotas.dec(channel.socket.getInetAddress)
      swallowError(channel.socket().close())
      swallowError(channel.close())
    }
  }
}

/**
 * Thread that accepts and configures new connections. There is one of these per endpoint.
 */
// 主要是接收客户端建立的连接请求，创建Socket连接并分配给processor处理
private[kafka] class Acceptor(val endPoint: EndPoint,
                              val sendBufferSize: Int,
                              val recvBufferSize: Int,
                              brokerId: Int,
                              processors: Array[Processor],
                              connectionQuotas: ConnectionQuotas) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

  // NIOSelector注意区分KSelector
  private val nioSelector = NSelector.open()
  // 打开一个server socket去监听9092端口
  // 用于接收客户端请求的ServerSocketChannel对象
  val serverChannel = openServerSocket(endPoint.host, endPoint.port)

  this.synchronized {
    // 真正创建3个process线程，并启动
    processors.foreach { processor =>
      Utils.newThread("kafka-network-thread-%d-%s-%d".format(brokerId, endPoint.protocolType.toString, processor.id), processor, false).start()
    }
  }

  /**
   * Accept loop that checks for new connection attempts
   */
  def run() {
    // 把nioSelector注册到ServerSocketChannel上，并且关注Accept事件，等待连接
    serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT)
    startupComplete()// 标识着当前线程启动操作已经完成
    try {
      var currentProcessor = 0
      while (isRunning) {
        try {
          val ready = nioSelector.select(500)// 等待关注的事件
          if (ready > 0) {
            val keys = nioSelector.selectedKeys()
            val iter = keys.iterator()
            while (iter.hasNext && isRunning) {
              try {
                val key = iter.next
                iter.remove()
                if (key.isAcceptable)// 调用accept()处理OP_ACCEPT事件
                  // 建立socketChannel，并转发给processor线程
                  accept(key, processors(currentProcessor))
                else// 如果不是OP_ACCEPT，报错
                  throw new IllegalStateException("Unrecognized key state for acceptor thread.")

                // round robin to the next processor thread
                // 使用轮洵的方式选择下一个processor
                currentProcessor = (currentProcessor + 1) % processors.length
              } catch {
                case e: Throwable => error("Error while accepting connection", e)
              }
            }
          }
        }
        catch {
          // We catch all the throwables to prevent the acceptor thread from exiting on exceptions due
          // to a select operation on a specific channel or a bad request. We don't want the
          // the broker to stop responding to requests from other clients in these scenarios.
          case e: ControlThrowable => throw e
          case e: Throwable => error("Error occurred", e)
        }
      }
    } finally {
      debug("Closing server socket and selector.")
      swallowError(serverChannel.close())
      swallowError(nioSelector.close())
      shutdownComplete()// 标识此线程的关闭操作已经完成
    }
  }

  /*
   * Create a server socket to listen for connections on.
   */
  private def openServerSocket(host: String, port: Int): ServerSocketChannel = {
    val socketAddress =
      if(host == null || host.trim.isEmpty)
        new InetSocketAddress(port)
      else
        new InetSocketAddress(host, port)
    val serverChannel = ServerSocketChannel.open()
    serverChannel.configureBlocking(false)
    serverChannel.socket().setReceiveBufferSize(recvBufferSize)
    try {
      serverChannel.socket.bind(socketAddress)
      info("Awaiting socket connections on %s:%d.".format(socketAddress.getHostString, serverChannel.socket.getLocalPort))
    } catch {
      case e: SocketException =>
        throw new KafkaException("Socket server failed to bind to %s:%d: %s.".format(socketAddress.getHostString, port, e.getMessage), e)
    }
    serverChannel
  }

  /*
   * Accept a new connection
   */
  // 处理OP_ACCEPT事件，创建SocketChannel并将其交给Processor.accept()处理
  def accept(key: SelectionKey, processor: Processor) {
    val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]
    val socketChannel = serverSocketChannel.accept()
    try {
      connectionQuotas.inc(socketChannel.socket().getInetAddress)
      socketChannel.configureBlocking(false)// 非阻塞
      socketChannel.socket().setTcpNoDelay(true)// 立即发送tcp请求，不等待
      socketChannel.socket().setKeepAlive(true)// 长连接
      socketChannel.socket().setSendBufferSize(sendBufferSize)// 设置socket发送的buffer大小

      debug("Accepted connection from %s on %s and assigned it to processor %d, sendBufferSize [actual|requested]: [%d|%d] recvBufferSize [actual|requested]: [%d|%d]"
            .format(socketChannel.socket.getRemoteSocketAddress, socketChannel.socket.getLocalSocketAddress, processor.id,
                  socketChannel.socket.getSendBufferSize, sendBufferSize,
                  socketChannel.socket.getReceiveBufferSize, recvBufferSize))

      // 建立socketChannel，并且转发给processor线程
      processor.accept(socketChannel)
    } catch {
      case e: TooManyConnectionsException =>
        info("Rejected connection from %s, address already has the configured maximum of %d connections.".format(e.ip, e.count))
        close(socketChannel)
    }
  }

  /**
   * Wakeup the thread for selection.
   */
  @Override
  def wakeup = nioSelector.wakeup()

}

/**
 * Thread that processes all requests from a single connection. There are N of these running in parallel
 * each of which has its own selector
 */
// 主要用于读取请求和写会响应的操作
private[kafka] class Processor(val id: Int,
                               time: Time,
                               maxRequestSize: Int,
                               requestChannel: RequestChannel,
                               connectionQuotas: ConnectionQuotas,
                               connectionsMaxIdleMs: Long,
                               protocol: SecurityProtocol,
                               channelConfigs: java.util.Map[String, _],
                               metrics: Metrics) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

  private object ConnectionId {
    def fromString(s: String): Option[ConnectionId] = s.split("-") match {
      case Array(local, remote) => BrokerEndPoint.parseHostPort(local).flatMap { case (localHost, localPort) =>
        BrokerEndPoint.parseHostPort(remote).map { case (remoteHost, remotePort) =>
          ConnectionId(localHost, localPort, remoteHost, remotePort)
        }
      }
      case _ => None
    }
  }

  private case class ConnectionId(localHost: String, localPort: Int, remoteHost: String, remotePort: Int) {
    override def toString: String = s"$localHost:$localPort-$remoteHost:$remotePort"
  }

  // 保存了由此Processor线程处理的新建的SocketChannel
  private val newConnections = new ConcurrentLinkedQueue[SocketChannel]()
  // 保存未发送的响应
  // 注意，客户端的inflightRequests中的请求是在收到响应后才移除
  // 而，服务端的inflightResponses中的响应会在发送成功后移除
  private val inflightResponses = mutable.Map[String, RequestChannel.Response]()
  private val metricTags = Map("networkProcessor" -> id.toString).asJava

  newGauge("IdlePercent",
    new Gauge[Double] {
      def value = {
        metrics.metrics().get(metrics.metricName("io-wait-ratio", "socket-server-metrics", metricTags)).value()
      }
    },
    metricTags.asScala
  )

  // KSelector类型，负责管理网络连接
  private val selector = new KSelector(
    maxRequestSize,
    connectionsMaxIdleMs,
    metrics,
    time,
    "socket-server",
    metricTags,
    false,
    ChannelBuilders.create(protocol, Mode.SERVER, LoginType.SERVER, channelConfigs, null, true))

  override def run() {
    startupComplete()// 标识Processor的初始化流程已经结束，唤醒阻塞等待此Processor初始化的线程
    while (isRunning) {
      // 当请求放入RequestChannel.requestQueue后，会有多个handler线程并发处理从其中取出请求处理，
      // 那么如何保证客户端请求的顺序呢？
      // 在这里通过多次关注/取消OP_READ事件和关注/取消OP_WRITE事件的操作，通过这些操作的组合，可以保证
      // 每个连接上只有一个请求和一个对应的响应，从而实现请求的顺序性
      try {
        // setup any new connections that have been queued up
        // 处理newConnections队列中新建的SocketChannel，队列中每个SocketChannel都要在NIOSelector上注册OP_READ事件
        configureNewConnections()
        // register any new responses for writing
        // 获取RequestChannel中对应的responseQueue队列，并处理其中缓存的response
        processNewResponses()
        // 负责读取请求，发送响应。底层调用的是KSelector.poll()
        // 每次调用poll都会将读取的请求、发送成功的请求以及断开的连接，放入completedReceives、completedSends、disconnected队列中处理
        poll()
        // 处理完成接收的请求
        // 处理KSelector.completedReceives队列
        processCompletedReceives()
        // 处理完成发送的响应
        // 处理KSelector.completedSends队列
        processCompletedSends()
        // 处理客户端断开连接的
        // 处理KSelector.disconnected队列
        processDisconnected()
      } catch {
        // We catch all the throwables here to prevent the processor thread from exiting. We do this because
        // letting a processor exit might cause a bigger impact on the broker. Usually the exceptions thrown would
        // be either associated with a specific socket channel or a bad request. We just ignore the bad socket channel
        // or request. This behavior might need to be reviewed if we see an exception that need the entire broker to stop.
        case e: ControlThrowable => throw e
        case e: Throwable =>
          error("Processor got uncaught exception.", e)
      }
    }

    debug("Closing selector - processor " + id)
    swallowError(closeAll())
    shutdownComplete()// 执行一系列关闭
  }

  private def processNewResponses() {
    // 获取requestChannel中responseQueues的response
    var curr = requestChannel.receiveResponse(id)
    while (curr != null) {
      try {
        curr.responseAction match {
          case RequestChannel.NoOpAction =>
            // There is no response to send to the client, we need to read more pipelined requests
            // that are sitting in the server's socket buffer
            // 如果是NoOpAction类型，表示此连接暂无响应需要发送，为KafkaChannel关注OP_READ事件
            curr.request.updateRequestMetrics
            trace("Socket server received empty response to send, registering for read: " + curr)
            selector.unmute(curr.request.connectionId)
          case RequestChannel.SendAction =>
            // 如果是SendAction类型，表示该response需要发送给客户端，为KafkaChannel关注OP_WRITE事件
            sendResponse(curr)
          case RequestChannel.CloseConnectionAction =>
            // 关闭连接
            curr.request.updateRequestMetrics
            trace("Closing socket connection actively according to the response code.")
            close(selector, curr.request.connectionId)
        }
      } finally {
        curr = requestChannel.receiveResponse(id)
      }
    }
  }

  /* `protected` for test usage */
  protected[network] def sendResponse(response: RequestChannel.Response) {
    trace(s"Socket server received response to send, registering for write and sending data: $response")
    val channel = selector.channel(response.responseSend.destination)
    // `channel` can be null if the selector closed the connection because it was idle for too long
    if (channel == null) {
      warn(s"Attempting to send response via channel for which there is no open connection, connection id $id")
      response.request.updateRequestMetrics()
    }
    else {
      selector.send(response.responseSend)
      inflightResponses += (response.request.connectionId -> response)
    }
  }

  private def poll() {
    try selector.poll(300)
    catch {
      case e @ (_: IllegalStateException | _: IOException) =>
        error(s"Closing processor $id due to illegal state or IO exception")
        swallow(closeAll())
        shutdownComplete()
        throw e
    }
  }

  private def processCompletedReceives() {
    // 遍历KSelector.completedReceives队列
    selector.completedReceives.asScala.foreach { receive =>
      try {
        // 获取请求对应的KafkaChannel
        val channel = selector.channel(receive.source)
        // 创建KafkaChannel对应的Session对象，与权限控制相关
        val session = RequestChannel.Session(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, channel.principal.getName),
          channel.socketAddress)
        // 将NetworkRecieve、ProcessorId、身份认证信息封装成RequestChannel.Request对象
        val req = RequestChannel.Request(processor = id, connectionId = receive.source, session = session, buffer = receive.payload, startTimeMs = time.milliseconds, securityProtocol = protocol)
        // 把request放到requestChannel.requestQueue队列中
        // 然后工作线程再去requestQueue队列中取，执行逻辑
        requestChannel.sendRequest(req)
        // 取消关注OP_READ事件，该连接不在读取数据
        selector.mute(receive.source)
      } catch {
        case e @ (_: InvalidRequestException | _: SchemaException) =>
          // note that even though we got an exception, we can assume that receive.source is valid. Issues with constructing a valid receive object were handled earlier
          error(s"Closing socket for ${receive.source} because of error", e)
          close(selector, receive.source)
      }
    }
  }

  private def processCompletedSends() {
    // 遍历KSelector.completedSends队列
    selector.completedSends.asScala.foreach { send =>
      // 此响应已经发送出去，从inflightResponses中删除
      val resp = inflightResponses.remove(send.destination).getOrElse {
        throw new IllegalStateException(s"Send for ${send.destination} completed, but not in `inflightResponses`")
      }
      resp.request.updateRequestMetrics()
      // 关注OP_READ事件，允许此连接继续读取数据
      selector.unmute(send.destination)
    }
  }

  private def processDisconnected() {
    // 遍历KSelector.disconnected队列
    selector.disconnected.asScala.foreach { connectionId =>
      val remoteHost = ConnectionId.fromString(connectionId).getOrElse {
        throw new IllegalStateException(s"connectionId has unexpected format: $connectionId")
      }.remoteHost
      // 从inflightResponses删除该连接对应的所有Response
      inflightResponses.remove(connectionId).foreach(_.request.updateRequestMetrics())
      // the channel has been closed by the selector but the quotas still need to be updated
      connectionQuotas.dec(InetAddress.getByName(remoteHost))
    }
  }

  /**
   * Queue up a new connection for reading
   */
  def accept(socketChannel: SocketChannel) {
    // 将acceptor线程转发过来的socketChannel添加到一个连接队列中
    newConnections.add(socketChannel)
    // 唤醒Processor线程，run方法中configureNewConnections处理队列中的socketChannel
    wakeup()
  }

  /**
   * Register any new connections that have been queued up
   */
  private def configureNewConnections() {
    while (!newConnections.isEmpty) {
      // 从队列中取出一个SocketChannel，之前Acceptor线程accept转发给Processor线程时，添加进去的
      val channel = newConnections.poll()
      try {
        debug(s"Processor $id listening to new connection from ${channel.socket.getRemoteSocketAddress}")
        val localHost = channel.socket().getLocalAddress.getHostAddress
        val localPort = channel.socket().getLocalPort
        val remoteHost = channel.socket().getInetAddress.getHostAddress
        val remotePort = channel.socket().getPort
        val connectionId = ConnectionId(localHost, localPort, remoteHost, remotePort).toString
        // 把socketChannel注册到selector上，并关注OP_READ事件
        selector.register(connectionId, channel)
      } catch {
        // We explicitly catch all non fatal exceptions and close the socket to avoid a socket leak. The other
        // throwables will be caught in processor and logged as uncaught exceptions.
        case NonFatal(e) =>
          // need to close the channel here to avoid a socket leak.
          close(channel)
          error(s"Processor $id closed connection from ${channel.getRemoteAddress}", e)
      }
    }
  }

  /**
   * Close the selector and all open connections
   */
  private def closeAll() {
    selector.channels.asScala.foreach { channel =>
      close(selector, channel.id)
    }
    selector.close()
  }

  /* For test usage */
  private[network] def channel(connectionId: String): Option[KafkaChannel] =
    Option(selector.channel(connectionId))

  /**
   * Wakeup the thread for selection.
   */
  @Override
  def wakeup = selector.wakeup()

}

class ConnectionQuotas(val defaultMax: Int, overrideQuotas: Map[String, Int]) {

  private val overrides = overrideQuotas.map { case (host, count) => (InetAddress.getByName(host), count) }
  private val counts = mutable.Map[InetAddress, Int]()

  def inc(address: InetAddress) {
    counts.synchronized {
      val count = counts.getOrElseUpdate(address, 0)
      counts.put(address, count + 1)
      val max = overrides.getOrElse(address, defaultMax)
      if (count >= max)
        throw new TooManyConnectionsException(address, max)
    }
  }

  def dec(address: InetAddress) {
    counts.synchronized {
      val count = counts.getOrElse(address,
        throw new IllegalArgumentException(s"Attempted to decrease connection count for address with no connections, address: $address"))
      if (count == 1)
        counts.remove(address)
      else
        counts.put(address, count - 1)
    }
  }

  def get(address: InetAddress): Int = counts.synchronized {
    counts.getOrElse(address, 0)
  }

}

class TooManyConnectionsException(val ip: InetAddress, val count: Int) extends KafkaException("Too many connections from %s (maximum = %d)".format(ip, count))
