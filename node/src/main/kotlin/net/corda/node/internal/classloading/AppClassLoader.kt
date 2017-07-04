package net.corda.node.internal.classloading

import java.net.URL

class AppClassLoader(val version: Int, urls: Array<URL>): ParentLastClassLoader(urls)