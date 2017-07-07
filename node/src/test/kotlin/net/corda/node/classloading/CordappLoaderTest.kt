package net.corda.node.classloading

import net.corda.core.flows.FlowLogic
import net.corda.core.flows.InitiatedBy
import net.corda.node.internal.classloading.CordappLoader
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import java.net.URLClassLoader
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
    @After
    fun cleanup() {
        System.clearProperty("net.corda.node.cordapp.scan.package")
    }

    @Test
    fun `test that classes that aren't in cordapps aren't loaded`() {
        // Basedir will not be a corda node directory so the dummy flow shouldn't be recognised as a part of a cordapp
        val loader = CordappLoader(Paths.get("."), true)
        Assert.assertNull(loader.findInitiatedFlows().find { it == LoaderTestFlow::class })
    }

    @Test
    fun `test that classes that are in a cordapp are loaded`() {
        System.setProperty("net.corda.node.cordapp.scan.package", "net.corda.node.classloading")
        val loader = CordappLoader(Paths.get("build/classes"), true)
        Assert.assertNotNull(loader.findInitiatedFlows().find { it == LoaderTestFlow::class })

    }

}