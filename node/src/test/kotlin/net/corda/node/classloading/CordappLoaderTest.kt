package net.corda.node.classloading

import net.corda.core.flows.FlowLogic
import net.corda.core.flows.InitiatedBy
import net.corda.node.internal.classloading.CordappLoader
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import java.nio.file.Path
import java.nio.file.Paths

class DummyFlow : FlowLogic<Unit>() {
    override fun call() { }
}

@InitiatedBy(DummyFlow::class)
class LoaderTestFlow : FlowLogic<Unit>() {
    override fun call() { }
}

class CordappLoaderTest {
    val loader = CordappLoader(Paths.get("."), true)

    @Before
    fun setup() {
        System.setProperty("net.corda.node.cordapp.scan.package", ".")
    }

    @After
    fun teardown() {
        System.setProperty("net.corda.node.cordapp.scan.package", null)
    }

    @Test
    fun `test that the classloader loads annotated classes`() {
        Assert.assertNotNull(loader.findInitiatedFlows().find { it == LoaderTestFlow::class })
    }

}