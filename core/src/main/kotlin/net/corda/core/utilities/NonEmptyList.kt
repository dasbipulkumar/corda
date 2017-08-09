package net.corda.core.utilities

import com.google.common.collect.Lists
import java.util.*
import java.util.function.Consumer
import java.util.stream.Stream
import kotlin.collections.ArrayList

/**
 * An immutable non-empty list.
 */
class NonEmptyList<T> private constructor(private val elements: List<T>) : List<T> by elements, RandomAccess {
    companion object {
        /**
         * Returns a singleton list containing [element]. This behaves the same as [Collections.singletonList] but returns
         * a [NonEmptyList] for the extra type-safety.
         */
        @JvmStatic
        fun <T> of(element: T): NonEmptyList<T> = NonEmptyList(Collections.singletonList(element))

        /** Returns a non-empty list containing the given elements in the order each was specified. */
        @JvmStatic
        fun <T> of(first: T, second: T, vararg rest: T): NonEmptyList<T> = NonEmptyList(Lists.asList(first, second, rest))

        /**
         * Returns a non-empty list containing each of [elements] in the order each appears first in the source collection.
         * @throws IllegalArgumentException If [elements] is empty.
         */
        @JvmStatic
        fun <T> copyOf(elements: Collection<T>): NonEmptyList<T> {
            if (elements is NonEmptyList) return elements
            return when (elements.size) {
                0 -> throw IllegalArgumentException("elements is empty")
                1 -> of(elements.first())
                else -> NonEmptyList(ArrayList(elements))
            }
        }
    }

    /** Returns the first element of the set. */
    fun head(): T = elements[0]
    fun tail(): List<T> = subList(1, elements.size)
    override fun isEmpty(): Boolean = false
    override fun subList(fromIndex: Int, toIndex: Int): List<T> = elements.subList(fromIndex, toIndex)
    override fun iterator(): Iterator<T> = listIterator(0)
    override fun listIterator(): ListIterator<T> = listIterator(0)
    override fun listIterator(index: Int): ListIterator<T> = object : ListIterator<T> by elements.listIterator(index) {}

    // Following methods are not delegated by Kotlin's Class delegation
    override fun forEach(action: Consumer<in T>) = elements.forEach(action)
    override fun stream(): Stream<T> = elements.stream()
    override fun parallelStream(): Stream<T> = elements.parallelStream()
    override fun spliterator(): Spliterator<T> = elements.spliterator()

    override fun equals(other: Any?): Boolean = other === this || other == elements
    override fun hashCode(): Int = elements.hashCode()
    override fun toString(): String = elements.toString()
}
