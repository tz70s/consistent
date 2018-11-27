package consistent

import org.scalatest.{FlatSpec, Matchers}

import scala.collection.SortedMap

class ConsistentHashSpec extends FlatSpec with Matchers {

  behavior of "Consistent Hash"

  it should "Construct a consistent hash with several nodes" in {
    import ConsistentHash._
    val cHash = ConsistentHash(Seq("node1", "node2", "node3"), 1)
    cHash.nodes shouldBe SortedMap(crc32HashOf("node1-1") -> "node1",
                                   crc32HashOf("node2-1") -> "node2",
                                   crc32HashOf("node3-1") -> "node3")
  }

  it should "Correctly gives a value from consistent hashing" in {
    val nodes = Seq("node1", "node2", "node3")
    val cHash = ConsistentHash(nodes)
    val node = cHash.nodeOf("fixture data")
    nodes.contains(node) shouldBe true
  }
}
