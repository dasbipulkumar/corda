package net.corda.node.services.messaging

import net.corda.core.concurrent.CordaFuture
import net.corda.core.crypto.random63BitValue
import net.corda.core.internal.concurrent.andForget
import net.corda.core.internal.concurrent.thenMatch
import net.corda.core.internal.ThreadBox
import net.corda.core.messaging.CordaRPCOps
import net.corda.core.messaging.MessageRecipients
import net.corda.core.messaging.RPCOps
import net.corda.core.messaging.SingleMessageRecipient
import net.corda.core.node.services.PartyInfo
import net.corda.core.node.services.TransactionVerifierService
import net.corda.core.transactions.LedgerTransaction
import net.corda.core.utilities.*
import net.corda.node.VersionInfo
import net.corda.node.services.RPCUserService
import net.corda.node.services.api.MonitoringService
import net.corda.node.services.config.NodeConfiguration
import net.corda.node.services.config.VerifierType
import net.corda.node.services.statemachine.StateMachineManager
import net.corda.node.services.transactions.InMemoryTransactionVerifierService
import net.corda.node.services.transactions.OutOfProcessTransactionVerifierService
import net.corda.node.utilities.*
import net.corda.nodeapi.ArtemisMessagingComponent
import net.corda.nodeapi.ArtemisTcpTransport
import net.corda.nodeapi.ConnectionDirection
import net.corda.nodeapi.VerifierApi
import net.corda.nodeapi.VerifierApi.VERIFICATION_REQUESTS_QUEUE_NAME
import net.corda.nodeapi.VerifierApi.VERIFICATION_RESPONSES_QUEUE_NAME_PREFIX
import org.apache.activemq.artemis.api.core.ActiveMQObjectClosedException
import org.apache.activemq.artemis.api.core.Message.*
import org.apache.activemq.artemis.api.core.RoutingType
import org.apache.activemq.artemis.api.core.SimpleString
import org.apache.activemq.artemis.api.core.client.*
import org.apache.activemq.artemis.api.core.client.ActiveMQClient.DEFAULT_ACK_BATCH_SIZE
import org.apache.activemq.artemis.api.core.management.ActiveMQServerControl
import org.bouncycastle.asn1.x500.X500Name
import org.jetbrains.exposed.sql.ResultRow
import org.jetbrains.exposed.sql.statements.InsertStatement
import java.security.PublicKey
import java.time.Instant
import java.util.*
import java.util.concurrent.*
import javax.annotation.concurrent.ThreadSafe

// TODO: Stop the wallet explorer and other clients from using this class and get rid of persistentInbox

/**
 * This class implements the [MessagingService] API using Apache Artemis, the successor to their ActiveMQ product.
 * Artemis is a message queue broker and here we run a client connecting to the specified broker instance
 * [ArtemisMessagingServer]. It's primarily concerned with peer-to-peer messaging.
 *
 * Message handlers are run on the provided [AffinityExecutor] synchronously, that is, the Artemis callback threads
 * are blocked until the handler is scheduled and completed. This allows backpressure to propagate from the given
 * executor through into Artemis and from there, back through to senders.
 *
 * An implementation of [CordaRPCOps] can be provided. If given, clients using the CordaMQClient RPC library can
 * invoke methods on the provided implementation. There is more documentation on this in the docsite and the
 * CordaRPCClient class.
 *
 * @param serverAddress The address of the broker instance to connect to (might be running in the same process).
 * @param myIdentity Either the public key to be used as the ArtemisMQ address and queue name for the node globally, or null to indicate
 * that this is a NetworkMapService node which will be bound globally to the name "networkmap".
 * @param nodeExecutor An executor to run received message tasks upon.
 * @param advertisedAddress The node address for inbound connections, advertised to the network map service and peers.
 * If not provided, will default to [serverAddress].
 */
@ThreadSafe
class NodeMessagingClient(override val config: NodeConfiguration,
                          val versionInfo: VersionInfo,
                          val serverAddress: NetworkHostAndPort,
                          val myIdentity: PublicKey?,
                          val nodeExecutor: AffinityExecutor.ServiceAffinityExecutor,
                          val database: CordaPersistence,
                          val networkMapRegistrationFuture: CordaFuture<Unit>,
                          val monitoringService: MonitoringService,
                          advertisedAddress: NetworkHostAndPort = serverAddress
) : ArtemisMessagingComponent(), MessagingService {
    companion object {
        private val log = loggerFor<NodeMessagingClient>()

        // This is a "property" attached to an Artemis MQ message object, which contains our own notion of "topic".
        // We should probably try to unify our notion of "topic" (really, just a string that identifies an endpoint
        // that will handle messages, like a URL) with the terminology used by underlying MQ libraries, to avoid
        // confusion.
        private val topicProperty = SimpleString("platform-topic")
        private val sessionIdProperty = SimpleString("session-id")
        private val cordaVendorProperty = SimpleString("corda-vendor")
        private val releaseVersionProperty = SimpleString("release-version")
        private val platformVersionProperty = SimpleString("platform-version")
        private val amqDelayMillis = System.getProperty("amq.delivery.delay.ms", "0").toInt()
        private val verifierResponseAddress = "$VERIFICATION_RESPONSES_QUEUE_NAME_PREFIX.${random63BitValue()}"

        private val messageMaxRetryCount: Int = 3
    }

    private class InnerState {
        var started = false
        var running = false
        var producer: ClientProducer? = null
        var p2pConsumer: ClientConsumer? = null
        var session: ClientSession? = null
        var sessionFactory: ClientSessionFactory? = null
        var rpcServer: RPCServer? = null
        // Consumer for inbound client RPC messages.
        var verificationResponseConsumer: ClientConsumer? = null
    }

    val messagesToRedeliver = database.transaction {
        JDBCHashMap<Long, Pair<Message, MessageRecipients>>("${NODE_DATABASE_PREFIX}message_retry", true)
    }

    val scheduledMessageRedeliveries = ConcurrentHashMap<Long, ScheduledFuture<*>>()

    val verifierService = when (config.verifierType) {
        VerifierType.InMemory -> InMemoryTransactionVerifierService(numberOfWorkers = 4)
        VerifierType.OutOfProcess -> createOutOfProcessVerifierService()
    }

    /** A registration to handle messages of different types */
    data class Handler(val topicSession: TopicSession,
                       val callback: (ReceivedMessage, MessageHandlerRegistration) -> Unit) : MessageHandlerRegistration

    private val cordaVendor = SimpleString(versionInfo.vendor)
    private val releaseVersion = SimpleString(versionInfo.releaseVersion)
    /** An executor for sending messages */
    private val messagingExecutor = AffinityExecutor.ServiceAffinityExecutor("Messaging", 1)

    /**
     * Apart from the NetworkMapService this is the only other address accessible to the node outside of lookups against the NetworkMapCache.
     */
    override val myAddress: SingleMessageRecipient = if (myIdentity != null) {
        NodeAddress.asPeer(myIdentity, advertisedAddress)
    } else {
        NetworkMapAddress(advertisedAddress)
    }

    private val state = ThreadBox(InnerState())
    private val handlers = CopyOnWriteArrayList<Handler>()

    private object Table : JDBCHashedTable("${NODE_DATABASE_PREFIX}message_ids") {
        val uuid = uuidString("message_id")
    }

    private val processedMessages: MutableSet<UUID> = Collections.synchronizedSet(
            object : AbstractJDBCHashSet<UUID, Table>(Table, loadOnInit = true) {
                override fun elementFromRow(row: ResultRow): UUID = row[table.uuid]
                override fun addElementToInsert(insert: InsertStatement, entry: UUID, finalizables: MutableList<() -> Unit>) {
                    insert[table.uuid] = entry
                }
            }
    )

    fun start(rpcOps: RPCOps, userService: RPCUserService) {
        state.locked {
            check(!started) { "start can't be called twice" }
            started = true

            log.info("Connecting to message broker: $serverAddress")
            // TODO Add broker CN to config for host verification in case the embedded broker isn't used
            val tcpTransport = ArtemisTcpTransport.tcpTransport(ConnectionDirection.Outbound(), serverAddress, config)
            val locator = ActiveMQClient.createServerLocatorWithoutHA(tcpTransport)
            // Never time out on our loopback Artemis connections. If we switch back to using the InVM transport this
            // would be the default and the two lines below can be deleted.
            locator.connectionTTL = -1
            locator.clientFailureCheckPeriod = -1
            locator.minLargeMessageSize = ArtemisMessagingServer.MAX_FILE_SIZE
            sessionFactory = locator.createSessionFactory()

            // Login using the node username. The broker will authentiate us as its node (as opposed to another peer)
            // using our TLS certificate.
            // Note that the acknowledgement of messages is not flushed to the Artermis journal until the default buffer
            // size of 1MB is acknowledged.
            val session = sessionFactory!!.createSession(NODE_USER, NODE_USER, false, true, true, locator.isPreAcknowledge, DEFAULT_ACK_BATCH_SIZE)
            this.session = session
            session.start()

            // Create a general purpose producer.
            val producer = session.createProducer()
            this.producer = producer

            // Create a queue, consumer and producer for handling P2P network messages.
            p2pConsumer = makeP2PConsumer(session, true)
            networkMapRegistrationFuture.thenMatch({
                state.locked {
                    log.info("Network map is complete, so removing filter from P2P consumer.")
                    try {
                        p2pConsumer!!.close()
                    } catch(e: ActiveMQObjectClosedException) {
                        // Ignore it: this can happen if the server has gone away before we do.
                    }
                    p2pConsumer = makeP2PConsumer(session, false)
                }
            }, {})

            rpcServer = RPCServer(rpcOps, NODE_USER, NODE_USER, locator, userService, config.myLegalName)

            fun checkVerifierCount() {
                if (session.queueQuery(SimpleString(VERIFICATION_REQUESTS_QUEUE_NAME)).consumerCount == 0) {
                    log.warn("No connected verifier listening on $VERIFICATION_REQUESTS_QUEUE_NAME!")
                }
            }

            if (config.verifierType == VerifierType.OutOfProcess) {
                createQueueIfAbsent(VerifierApi.VERIFICATION_REQUESTS_QUEUE_NAME)
                createQueueIfAbsent(verifierResponseAddress)
                verificationResponseConsumer = session.createConsumer(verifierResponseAddress)
                messagingExecutor.scheduleAtFixedRate(::checkVerifierCount, 0, 10, TimeUnit.SECONDS)
            }
        }

        resumeMessageRedelivery()
    }

    /**
     * We make the consumer twice, once to filter for just network map messages, and then once that is complete, we close
     * the original and make another without a filter.  We do this so that there is a network map in place for all other
     * message handlers.
     */
    private fun makeP2PConsumer(session: ClientSession, networkMapOnly: Boolean): ClientConsumer {
        return if (networkMapOnly) {
            // Filter for just the network map messages.
            val messageFilter = "hyphenated_props:$topicProperty like 'platform.network_map.%'"
            session.createConsumer(P2P_QUEUE, messageFilter)
        } else
            session.createConsumer(P2P_QUEUE)
    }

    private fun resumeMessageRedelivery() {
        messagesToRedeliver.forEach {
            retryId, (message, target) -> send(message, target, retryId)
        }
    }

    private val shutdownLatch = CountDownLatch(1)

    private fun processMessage(consumer: ClientConsumer): Boolean {
        // Two possibilities here:
        //
        // 1. We block waiting for a message and the consumer is closed in another thread. In this case
        //    receive returns null and we break out of the loop.
        // 2. We receive a message and process it, and stop() is called during delivery. In this case,
        //    calling receive will throw and we break out of the loop.
        //
        // It's safe to call into receive simultaneous with other threads calling send on a producer.
        val artemisMessage: ClientMessage = try {
            consumer.receive()
        } catch(e: ActiveMQObjectClosedException) {
            null
        } ?: return false

        val message: ReceivedMessage? = artemisToCordaMessage(artemisMessage)
        if (message != null)
            deliver(message)

        // Ack the message so it won't be redelivered. We should only really do this when there were no
        // transient failures. If we caught an exception in the handler, we could back off and retry delivery
        // a few times before giving up and redirecting the message to a dead-letter address for admin or
        // developer inspection. Artemis has the features to do this for us, we just need to enable them.
        //
        // TODO: Setup Artemis delayed redelivery and dead letter addresses.
        //
        // ACKing a message calls back into the session which isn't thread safe, so we have to ensure it
        // doesn't collide with a send here. Note that stop() could have been called whilst we were
        // processing a message but if so, it'll be parked waiting for us to count down the latch, so
        // the session itself is still around and we can still ack messages as a result.
        state.locked {
            artemisMessage.acknowledge()
        }
        return true
    }

    private fun runPreNetworkMap(serverControl: ActiveMQServerControl) {
        val consumer = state.locked {
            check(started) { "start must be called first" }
            check(!running) { "run can't be called twice" }
            running = true
            rpcServer!!.start(serverControl)
            (verifierService as? OutOfProcessTransactionVerifierService)?.start(verificationResponseConsumer!!)
            p2pConsumer!!
        }

        while (!networkMapRegistrationFuture.isDone && processMessage(consumer)) {
        }
        with(networkMapRegistrationFuture) {
            if (isDone) getOrThrow() else andForget(log) // Trigger node shutdown here to avoid deadlock in shutdown hooks.
        }
    }

    private fun runPostNetworkMap() {
        val consumer = state.locked {
            // If it's null, it means we already called stop, so return immediately.
            p2pConsumer ?: return
        }

        while (processMessage(consumer)) {
        }
    }

    /**
     * Starts the p2p event loop: this method only returns once [stop] has been called.
     *
     * This actually runs as two sequential loops. The first subscribes for and receives only network map messages until
     * we get our network map fetch response.  At that point the filtering consumer is closed and we proceed to the second loop and
     * consume all messages via a new consumer without a filter applied.
     */
    fun run(serverControl: ActiveMQServerControl) {
        try {
            // Build the network map.
            runPreNetworkMap(serverControl)
            // Process everything else once we have the network map.
            runPostNetworkMap()
        } finally {
            shutdownLatch.countDown()
        }
    }

    private fun artemisToCordaMessage(message: ClientMessage): ReceivedMessage? {
        try {
            val topic = message.required(topicProperty) { getStringProperty(it) }
            val sessionID = message.required(sessionIdProperty) { getLongProperty(it) }
            val user = requireNotNull(message.getStringProperty(HDR_VALIDATED_USER)) { "Message is not authenticated" }
            val platformVersion = message.required(platformVersionProperty) { getIntProperty(it) }
            // Use the magic deduplication property built into Artemis as our message identity too
            val uuid = message.required(HDR_DUPLICATE_DETECTION_ID) { UUID.fromString(message.getStringProperty(it)) }
            log.trace { "Received message from: ${message.address} user: $user topic: $topic sessionID: $sessionID uuid: $uuid" }

            return ArtemisReceivedMessage(TopicSession(topic, sessionID), X500Name(user), platformVersion, uuid, message)
        } catch (e: Exception) {
            log.error("Unable to process message, ignoring it: $message", e)
            return null
        }
    }

    private inline fun <T> ClientMessage.required(key: SimpleString, extractor: ClientMessage.(SimpleString) -> T): T {
        require(containsProperty(key)) { "Missing $key" }
        return extractor(key)
    }

    private class ArtemisReceivedMessage(override val topicSession: TopicSession,
                                         override val peer: X500Name,
                                         override val platformVersion: Int,
                                         override val uniqueMessageId: UUID,
                                         private val message: ClientMessage) : ReceivedMessage {
        override val data: ByteArray by lazy { ByteArray(message.bodySize).apply { message.bodyBuffer.readBytes(this) } }
        override val debugTimestamp: Instant get() = Instant.ofEpochMilli(message.timestamp)
        override fun toString() = "${topicSession.topic}#${data.sequence()}"
    }

    private fun deliver(msg: ReceivedMessage): Boolean {
        state.checkNotLocked()
        // Because handlers is a COW list, the loop inside filter will operate on a snapshot. Handlers being added
        // or removed whilst the filter is executing will not affect anything.
        val deliverTo = handlers.filter { it.topicSession.isBlank() || it.topicSession == msg.topicSession }
        try {
            // This will perform a BLOCKING call onto the executor. Thus if the handlers are slow, we will
            // be slow, and Artemis can handle that case intelligently. We don't just invoke the handler
            // directly in order to ensure that we have the features of the AffinityExecutor class throughout
            // the bulk of the codebase and other non-messaging jobs can be scheduled onto the server executor
            // easily.
            //
            // Note that handlers may re-enter this class. We aren't holding any locks and methods like
            // start/run/stop have re-entrancy assertions at the top, so it is OK.
            nodeExecutor.fetchFrom {
                database.transaction {
                    if (msg.uniqueMessageId in processedMessages) {
                        log.trace { "Discard duplicate message ${msg.uniqueMessageId} for ${msg.topicSession}" }
                    } else {
                        if (deliverTo.isEmpty()) {
                            // TODO: Implement dead letter queue, and send it there.
                            log.warn("Received message ${msg.uniqueMessageId} for ${msg.topicSession} that doesn't have any registered handlers yet")
                        } else {
                            callHandlers(msg, deliverTo)
                        }
                        // TODO We will at some point need to decide a trimming policy for the id's
                        processedMessages += msg.uniqueMessageId
                    }
                }
            }
        } catch(e: Exception) {
            log.error("Caught exception whilst executing message handler for ${msg.topicSession}", e)
        }
        return true
    }

    private fun callHandlers(msg: ReceivedMessage, deliverTo: List<Handler>) {
        for (handler in deliverTo) {
            handler.callback(msg, handler)
        }
    }

    override fun stop() {
        val running = state.locked {
            // We allow stop() to be called without a run() in between, but it must have at least been started.
            check(started)
            val prevRunning = running
            running = false
            val c = p2pConsumer ?: throw IllegalStateException("stop can't be called twice")
            try {
                c.close()
            } catch(e: ActiveMQObjectClosedException) {
                // Ignore it: this can happen if the server has gone away before we do.
            }
            p2pConsumer = null
            prevRunning
        }
        if (running && !nodeExecutor.isOnThread) {
            // Wait for the main loop to notice the consumer has gone and finish up.
            shutdownLatch.await()
        }
        // Only first caller to gets running true to protect against double stop, which seems to happen in some integration tests.
        if (running) {
            state.locked {
                producer?.close()
                producer = null
                // Ensure any trailing messages are committed to the journal
                session!!.commit()
                // Closing the factory closes all the sessions it produced as well.
                sessionFactory!!.close()
                sessionFactory = null
            }
        }
    }

    override fun send(message: Message, target: MessageRecipients, retryId: Long?) {
        // We have to perform sending on a different thread pool, since using the same pool for messaging and
        // fibers leads to Netty buffer memory leaks, caused by both Netty and Quasar fiddling with thread-locals.
        messagingExecutor.fetchFrom {
            state.locked {
                val mqAddress = getMQAddress(target)
                val artemisMessage = session!!.createMessage(true).apply {
                    putStringProperty(cordaVendorProperty, cordaVendor)
                    putStringProperty(releaseVersionProperty, releaseVersion)
                    putIntProperty(platformVersionProperty, versionInfo.platformVersion)
                    putStringProperty(topicProperty, SimpleString(message.topicSession.topic))
                    putLongProperty(sessionIdProperty, message.topicSession.sessionID)
                    writeBodyBufferBytes(message.data)
                    // Use the magic deduplication property built into Artemis as our message identity too
                    putStringProperty(HDR_DUPLICATE_DETECTION_ID, SimpleString(message.uniqueMessageId.toString()))

                    // For demo purposes - if set then add a delay to messages in order to demonstrate that the flows are doing as intended
                    if (amqDelayMillis > 0 && message.topicSession.topic == StateMachineManager.sessionTopic.topic) {
                        putLongProperty(HDR_SCHEDULED_DELIVERY_TIME, System.currentTimeMillis() + amqDelayMillis)
                    }
                }
                log.trace {
                    "Send to: $mqAddress topic: ${message.topicSession.topic} " +
                            "sessionID: ${message.topicSession.sessionID} uuid: ${message.uniqueMessageId}"
                }
                producer!!.send(mqAddress, artemisMessage)

                retryId?.let {
                    database.transaction {
                        messagesToRedeliver.computeIfAbsent(it, { Pair(message, target) })
                    }
                    scheduledMessageRedeliveries[it] = messagingExecutor.schedule({
                        sendWithRetry(0, mqAddress, artemisMessage, it)
                    }, config.messageRedeliveryDelaySeconds.toLong(), TimeUnit.SECONDS)

                }
            }
        }
    }

    private fun sendWithRetry(retryCount: Int, address: String, message: ClientMessage, retryId: Long) {
        fun ClientMessage.randomiseDuplicateId() {
            putStringProperty(HDR_DUPLICATE_DETECTION_ID, SimpleString(UUID.randomUUID().toString()))
        }

        log.trace { "Attempting to retry #$retryCount message delivery for $retryId" }
        if (retryCount >= messageMaxRetryCount) {
            log.warn("Reached the maximum number of retries ($messageMaxRetryCount) for message $message redelivery to $address")
            scheduledMessageRedeliveries.remove(retryId)
            return
        }

        message.randomiseDuplicateId()

        state.locked {
            log.trace { "Retry #$retryCount sending message $message to $address for $retryId" }
            producer!!.send(address, message)
        }

        scheduledMessageRedeliveries[retryId] = messagingExecutor.schedule({
            sendWithRetry(retryCount + 1, address, message, retryId)
        }, config.messageRedeliveryDelaySeconds.toLong(), TimeUnit.SECONDS)
    }

    override fun cancelRedelivery(retryId: Long) {
        database.transaction {
            messagesToRedeliver.remove(retryId)
        }
        scheduledMessageRedeliveries[retryId]?.let {
            log.trace { "Cancelling message redelivery for retry id $retryId" }
            if (!it.isDone) it.cancel(true)
            scheduledMessageRedeliveries.remove(retryId)
        }
    }

    private fun getMQAddress(target: MessageRecipients): String {
        return if (target == myAddress) {
            // If we are sending to ourselves then route the message directly to our P2P queue.
            P2P_QUEUE
        } else {
            // Otherwise we send the message to an internal queue for the target residing on our broker. It's then the
            // broker's job to route the message to the target's P2P queue.
            val internalTargetQueue = (target as? ArtemisAddress)?.queueName ?: throw IllegalArgumentException("Not an Artemis address")
            createQueueIfAbsent(internalTargetQueue)
            internalTargetQueue
        }
    }

    /** Attempts to create a durable queue on the broker which is bound to an address of the same name. */
    private fun createQueueIfAbsent(queueName: String) {
        state.alreadyLocked {
            val queueQuery = session!!.queueQuery(SimpleString(queueName))
            if (!queueQuery.isExists) {
                log.info("Create fresh queue $queueName bound on same address")
                session!!.createQueue(queueName, RoutingType.MULTICAST, queueName, true)
            }
        }
    }

    override fun addMessageHandler(topic: String,
                                   sessionID: Long,
                                   callback: (ReceivedMessage, MessageHandlerRegistration) -> Unit): MessageHandlerRegistration {
        return addMessageHandler(TopicSession(topic, sessionID), callback)
    }

    override fun addMessageHandler(topicSession: TopicSession,
                                   callback: (ReceivedMessage, MessageHandlerRegistration) -> Unit): MessageHandlerRegistration {
        require(!topicSession.isBlank()) { "Topic must not be blank, as the empty topic is a special case." }
        val handler = Handler(topicSession, callback)
        handlers.add(handler)
        return handler
    }

    override fun removeMessageHandler(registration: MessageHandlerRegistration) {
        handlers.remove(registration)
    }

    override fun createMessage(topicSession: TopicSession, data: ByteArray, uuid: UUID): Message {
        // TODO: We could write an object that proxies directly to an underlying MQ message here and avoid copying.
        return object : Message {
            override val topicSession: TopicSession = topicSession
            override val data: ByteArray = data
            override val debugTimestamp: Instant = Instant.now()
            override val uniqueMessageId: UUID = uuid
            override fun toString() = "$topicSession#${String(data)}"
        }
    }

    private fun createOutOfProcessVerifierService(): TransactionVerifierService {
        return object : OutOfProcessTransactionVerifierService(monitoringService) {
            override fun sendRequest(nonce: Long, transaction: LedgerTransaction) {
                messagingExecutor.fetchFrom {
                    state.locked {
                        val message = session!!.createMessage(false)
                        val request = VerifierApi.VerificationRequest(nonce, transaction, SimpleString(verifierResponseAddress))
                        request.writeToClientMessage(message)
                        producer!!.send(VERIFICATION_REQUESTS_QUEUE_NAME, message)
                    }
                }
            }

        }
    }

    override fun getAddressOfParty(partyInfo: PartyInfo): MessageRecipients {
        return when (partyInfo) {
            is PartyInfo.Node -> getArtemisPeerAddress(partyInfo.node)
            is PartyInfo.Service -> ServiceAddress(partyInfo.service.identity.owningKey)
        }
    }
}
