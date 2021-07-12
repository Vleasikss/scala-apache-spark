package org.example

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class SomeTest extends AnyFlatSpec with BeforeAndAfter {

  before {

  }
  after {

  }

  it should "print hello world" in {
    val greeting = "Hello world"
    println(greeting)
  }
}
