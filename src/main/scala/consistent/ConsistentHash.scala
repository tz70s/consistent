package consistent

import java.util
import java.util.zip.CRC32

import scala.collection.SortedMap
import scala.reflect.ClassTag

/**
 * Data structure for Consistent Hashing.
 *
 * The implementation detail inspired by akka's implementation.
 *
 * {{
 *   val cHash = ConsistentHash("node1, "node2")
 *   val scaleUp = cHash :+ "node3"
 *   val scaleDown = cHash :- "node3"
 *
 *   val dataPartition = cHash.nodeOf("some data")
 * }}
 *
 */
class ConsistentHash[T: ClassTag] private (private[consistent] val nodes: SortedMap[Long, T], val virtualNodes: Int) {

  import ConsistentHash._

  // Create temporary aligned data structures for fast binary search.
  // Optimization techniques from akka.
  private val (nodeHashRing: Array[Long], nodeRing: Array[T]) = {
    val (nhr: Seq[Long], nr: Seq[T]) = nodes.toSeq.unzip
    (nhr.toArray, nr.toArray)
  }

  def add(node: T): ConsistentHash[T] = {
    val nodePairs = replicate(node, virtualNodes).map(_ -> node)
    new ConsistentHash(nodes ++ nodePairs, virtualNodes)
  }

  def :+(node: T): ConsistentHash[T] = add(node)

  def remove(node: T): ConsistentHash[T] = {
    val hashSeq = replicate(node, virtualNodes)
    new ConsistentHash(nodes -- hashSeq, virtualNodes)
  }

  def :-(node: T): ConsistentHash[T] = remove(node)

  /** Pick up the key range within nodes. */
  def nodeOf(key: String): T = {
    val hashValue = crc32HashOf(key)
    pickClockwiseFirst(hashValue)
  }

  def nodeOf(key: Array[Byte]): T = {
    val hashValue = crc32HashOf(key)
    pickClockwiseFirst(hashValue)
  }

  private def pickClockwiseFirst(key: Long): T = {
    // Better bump into binarySearch JavaDoc.
    // The binarySearch method returns the -InsertionPoint - 1
    // Where InsertionPoint is the next element or
    // array's length if all items are less than search key.
    val idx = util.Arrays.binarySearch(nodeHashRing, key)
    val clockwiseFirst =
      if (idx > 0) idx // key match
      else {
        val ip = math.abs(idx + 1) // Insertion point.
        if (ip >= nodeHashRing.length) 0 // return the first element (clockwise)
        else ip
      }
    nodeRing(clockwiseFirst)
  }

  override def toString: String = nodes.toString
}

object ConsistentHash {

  def apply[T: ClassTag](nodes: Iterable[T], virtualNodes: Int = 20): ConsistentHash[T] = {

    // Transform into a sequence of virtual nodes.
    val sortedNodes = SortedMap.empty[Long, T] ++ nodes.flatMap { node =>
      replicate(node, virtualNodes).map(k => k -> node)
    }
    new ConsistentHash[T](sortedNodes, virtualNodes)
  }

  private def replicate[T](value: T, factor: Int) =
    (1 to factor).map(id => crc32HashOf(s"$value-$id"))

  /**
   * From some online references noted:
   *
   * CRC32 is faster than murmur hash due to hardware acceleration.
   * Slower than Java Hash but with less collision.
   */
  private[consistent] def crc32HashOf(value: String): Long =
    crc32HashOf(value.getBytes)

  private[consistent] def crc32HashOf(value: Array[Byte]): Long = {
    val crc = new CRC32()
    crc.update(value)
    crc.getValue
  }
}
