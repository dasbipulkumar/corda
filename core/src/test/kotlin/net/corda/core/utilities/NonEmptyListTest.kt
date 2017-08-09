package net.corda.core.utilities

import com.google.common.collect.Lists
import com.google.common.collect.testing.ListTestSuiteBuilder
import com.google.common.collect.testing.TestStringListGenerator
import com.google.common.collect.testing.features.CollectionFeature
import com.google.common.collect.testing.features.CollectionSize
import com.google.common.collect.testing.features.Feature
import junit.framework.TestSuite
import net.corda.core.serialization.deserialize
import net.corda.core.serialization.serialize
import net.corda.testing.initialiseTestSerialization
import net.corda.testing.resetTestSerialization
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Suite

@RunWith(Suite::class)
@Suite.SuiteClasses(
        NonEmptyListTest.SingletonFactory::class,
        NonEmptyListTest.AtLeastTwoElementFactory::class,
        NonEmptyListTest.CopyOfFactory::class,
        NonEmptyListTest.General::class
)
class NonEmptyListTest {
    private companion object {
        fun createGuavaTestSuite(name: String, generator: (Array<out String>) -> NonEmptyList<String>, vararg features: Feature<*>): TestSuite {
            return ListTestSuiteBuilder
                .using(object : TestStringListGenerator() {
                    override fun create(elements: Array<out String>) = generator(elements)
                })
                .named(name)
                .withFeatures(Lists.asList(CollectionFeature.ALLOWS_NULL_VALUES, features))
                .createTestSuite()
        }
    }

    object SingletonFactory {
        @JvmStatic
        fun suite(): TestSuite = createGuavaTestSuite("of[element]", { NonEmptyList.of(it.single()) }, CollectionSize.ONE)
    }

    object AtLeastTwoElementFactory {
        @JvmStatic
        fun suite(): TestSuite {
            return createGuavaTestSuite(
                "of[first, second, rest]",
                { NonEmptyList.of(it[0], it[1], *it.copyOfRange(2, it.size)) },
                CollectionSize.SEVERAL)
        }
    }

    object CopyOfFactory {
        @JvmStatic
        fun suite(): TestSuite {
            return createGuavaTestSuite("copyOf", { NonEmptyList.copyOf(it.asList()) }, CollectionSize.ONE, CollectionSize.SEVERAL)
        }
    }

    class General {
        @Test
        fun `copyOf - empty source`() {
            assertThatThrownBy { NonEmptyList.copyOf(ArrayList<Int>()) }.isInstanceOf(IllegalArgumentException::class.java)
        }

        @Test
        fun heads() {
            assertThat(NonEmptyList.of(1).head()).isEqualTo(1)
            assertThat(NonEmptyList.of(1, 2).head()).isEqualTo(1)
        }

        @Test
        fun tails() {
            assertThat(NonEmptyList.of(1).tail()).isEmpty()
            assertThat(NonEmptyList.of(1, 2).tail()).isEqualTo(listOf(2))
        }

        @Test
        fun `unmodfiable subList`() {
            assertThatThrownBy { (NonEmptyList.of(1).tail() as MutableList).add(2) }.isInstanceOf(UnsupportedOperationException::class.java)
            assertThatThrownBy { (NonEmptyList.of(1, 2).tail() as MutableList).add(2) }.isInstanceOf(UnsupportedOperationException::class.java)
        }

        @Test
        fun `serialize deserialize`() {
            initialiseTestSerialization()
            try {
                val original = NonEmptyList.of(-17, 22, 17)
                val copy = original.serialize().deserialize()
                assertThat(copy).isEqualTo(original).isNotSameAs(original)
            } finally {
                resetTestSerialization()
            }
        }
    }
}
