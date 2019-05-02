package org.apache.tuweni.jsonrpc

import io.vertx.core.Vertx
import kotlinx.coroutines.runBlocking
import org.apache.lucene.index.IndexWriter
import org.apache.tuweni.bytes.Bytes
import org.apache.tuweni.bytes.Bytes32
import org.apache.tuweni.eth.Address
import org.apache.tuweni.eth.Block
import org.apache.tuweni.eth.BlockBody
import org.apache.tuweni.eth.BlockHeader
import org.apache.tuweni.eth.Hash
import org.apache.tuweni.eth.repository.BlockchainIndex
import org.apache.tuweni.eth.repository.BlockchainRepository
import org.apache.tuweni.junit.BouncyCastleExtension
import org.apache.tuweni.junit.LuceneIndex
import org.apache.tuweni.junit.LuceneIndexWriter
import org.apache.tuweni.junit.LuceneIndexWriterExtension
import org.apache.tuweni.junit.VertxExtension
import org.apache.tuweni.junit.VertxInstance
import org.apache.tuweni.kv.MapKeyValueStore
import org.apache.tuweni.units.bigints.UInt256
import org.apache.tuweni.units.ethereum.Gas
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.extension.ExtendWith
import java.time.Instant
import java.time.temporal.ChronoUnit

@ExtendWith(VertxExtension::class, LuceneIndexWriterExtension::class, BouncyCastleExtension::class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JSONRPCClientTest {

  private var server : JSONRPCServer? = null

  @BeforeAll
  fun createJSONRPCServer(@VertxInstance vertx : Vertx, @LuceneIndexWriter indexWriter : IndexWriter) = runBlocking {
    val repository = BlockchainRepository(
      MapKeyValueStore(),
      MapKeyValueStore(),
      MapKeyValueStore(),
      MapKeyValueStore(),
      BlockchainIndex(indexWriter)
    )
    val genesisHeader = BlockHeader(
      Hash.fromBytes(Bytes32.random()),
      Hash.fromBytes(Bytes32.random()),
      Address.fromBytes(Bytes.random(20)),
      Hash.fromBytes(Bytes32.random()),
      Hash.fromBytes(Bytes32.random()),
      Hash.fromBytes(Bytes32.random()),
      Bytes32.random(),
      UInt256.fromBytes(Bytes32.random()),
      UInt256.valueOf(42L),
      Gas.valueOf(3000),
      Gas.valueOf(2000),
      Instant.now().plusSeconds(30).truncatedTo(ChronoUnit.SECONDS),
      Bytes.of(2, 3, 4, 5, 6, 7, 8, 9, 10),
      Hash.fromBytes(Bytes32.random()),
      Bytes32.random()
    )
    val genesisBlock = Block(genesisHeader, BlockBody(emptyList(), emptyList()))
    repository.storeBlock(genesisBlock)
      server = JSONRPCServer(vertx, "localhost", 10000, repository)
      server!!.start()
  }

  @AfterAll
  fun stopServer() = runBlocking {
    server!!.stop()
  }

  @Test
  fun canGetBlockNumber(@VertxInstance vertx : Vertx) = runBlocking {
    val client = JSONRPCClient(vertx,"localhost", 10000)
    val currentBlockNumber = client.blockNumber()

    assertEquals(42, currentBlockNumber)
  }

  @Test
  fun canGetBlock(@VertxInstance vertx : Vertx) = runBlocking {
    val client = JSONRPCClient(vertx,"localhost", 10000)
    val currentBlockNumber = client.getBlockByNumber(42, true)

    assertEquals(42, currentBlockNumber)
  }
}
