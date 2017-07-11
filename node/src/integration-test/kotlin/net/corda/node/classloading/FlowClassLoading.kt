package net.corda.node.classloading

import net.corda.core.flows.FlowLogic
import net.corda.core.flows.InitiatedBy
import net.corda.core.flows.InitiatingFlow
import net.corda.core.flows.StartableByRPC
import net.corda.core.getOrThrow
import net.corda.core.identity.Party
import net.corda.core.serialization.CordaSerializable
import net.corda.core.utilities.unwrap
import net.corda.node.internal.AbstractNode
import net.corda.node.internal.classloading.CordappLoader
import net.corda.testing.node.InMemoryMessagingNetwork
import net.corda.testing.node.MockNetwork
import net.corda.testing.node.MockNetwork.MockNode
import net.corda.testing.node.InMemoryMessagingNetwork.ServicePeerAllocationStrategy.RoundRobin
import org.junit.Assert
import org.junit.Test

class FlowClassLoading {
    private val mockNet = MockNetwork(servicePeerAllocationStrategy = RoundRobin())

    @CordaSerializable
    private data class Data(val value: Int)

    @InitiatingFlow
    private class Initiator(private val otherSide: Party) : FlowLogic<Data>() {
        override fun call(): Data {
            //Assert.assertEquals(loader.appClassLoader, javaClass.classLoader)
            send(otherSide, Data(0))
            val data: Data = receive<Data>(otherSide).unwrap { it }
            //Assert.assertEquals(loader.appClassLoader, data.javaClass.classLoader)
            return data
        }
    }

    @InitiatedBy(Initiator::class)
    private class Receiver(val otherSide: Party) : FlowLogic<Unit>() {
        override fun call() {
            //Assert.assertEquals(loader.appClassLoader, javaClass.classLoader)
            val data = receive<Data>(otherSide).unwrap { it }
            //Assert.assertEquals(loader.appClassLoader, data.javaClass.classLoader)
            send(otherSide, data)
        }
    }

    private fun createTwoMappedNodes(): Pair<AbstractNode, AbstractNode> {
        val nodes = mockNet.createTwoNodes()
        nodes.first.services.identityService.registerIdentity(nodes.second.info.legalIdentityAndCert)
        nodes.second.services.identityService.registerIdentity(nodes.first.info.legalIdentityAndCert)
        return nodes
    }

    @Test
    fun `flows from Cordapps use the correct classloader`() {
        // This line instructs the node to use this classpath for cordapps instead of the plugins dir
        System.setProperty("net.corda.node.cordapp.scan.package", "net.corda.node.classloading")

        val (nodeA, nodeB) = createTwoMappedNodes()
        val expected = nodeA.cordappLoader.appClassLoader

        val flow = Initiator(nodeB.info.legalIdentity)
        val resultFuture = nodeA.services.startFlow(flow).resultFuture
        mockNet.runNetwork()
        val data = resultFuture.getOrThrow()

        Assert.assertEquals(expected, data.javaClass.classLoader)
    }
}