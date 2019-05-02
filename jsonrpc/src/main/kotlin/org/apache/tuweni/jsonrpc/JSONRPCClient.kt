package org.apache.tuweni.jsonrpc

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.core.TreeNode
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.ObjectMapper
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers

import org.apache.tuweni.concurrent.AsyncResult
import org.apache.tuweni.concurrent.AsyncCompletion
import org.apache.tuweni.concurrent.CompletableAsyncResult
import java.lang.IllegalArgumentException
import java.lang.RuntimeException
import kotlin.coroutines.CoroutineContext
import io.vertx.core.json.Json.mapper
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.module.SimpleModule
import org.apache.tuweni.bytes.Bytes
import org.apache.tuweni.concurrent.coroutines.await
import org.apache.tuweni.eth.Address
import org.apache.tuweni.eth.Block
import org.apache.tuweni.eth.Hash
import org.apache.tuweni.eth.Transaction
import org.apache.tuweni.units.bigints.UInt256
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger


class JSONRPCClient(private val vertx : Vertx, private val host: String, private val port : Int,
                    override val coroutineContext: CoroutineContext = Dispatchers.Default
) : CoroutineScope {

  companion object {
    private val objectMapper = ObjectMapper().registerModule(JacksonJSONRPCModule())
    private val sequence = AtomicInteger(0)
  }

  private val httpClient = vertx.createHttpClient()

  init {
    if (port < 1 || port > 65535) {
      throw IllegalArgumentException("Invalid port")
    }
  }

  suspend fun <T> call(method : String, params : List<Any>, javaClass : Class<T>) : T? {
    val messageMap = mapOf(Pair("jsonrpc", "2.0"), Pair("method", method), Pair("params", params), Pair("id", sequence.incrementAndGet()))
    val message = objectMapper.writeValueAsBytes(messageMap)

    val result = AsyncResult.incomplete<T>()

    @Suppress("DEPRECATION")
    httpClient.post(port, host, "/").exceptionHandler({
      result.completeExceptionally(it)
    }).handler({ response ->
      if (response.statusCode() != 200) {
        result.completeExceptionally(JSONRPCError(response.statusMessage()))
      } else {
        response.bodyHandler({
          System.out.println(String(it.bytes))
          val root = mapper.readTree(it.bytes)
          result.complete(objectMapper.treeToValue(root.get("result") as TreeNode, javaClass))
        }).exceptionHandler({result.completeExceptionally(it)})
      }
    }).end(Buffer.buffer(message))
    result.await()
    return result.get()
  }

  suspend fun blockNumber(): Int {
    val result = call("eth_blockNumber", listOf(), String::class.java)
    return Integer.parseInt(result,16)
  }

  suspend fun getBlockByNumber(blockNumber: Int, fullTx : Boolean): Block? {
    val result = call("eth_getBlockByNumber", listOf(blockNumber.toString(16), fullTx), Block::class.java)
    return result
  }

}

class JSONRPCError(message: String?) : RuntimeException(message) {

}

internal class JacksonJSONRPCModule() : SimpleModule("jsonrpc") {

  init {
    addSerializer(Hash::class.java, HashJsonSerializer())
    addSerializer(Block::class.java, BlockJsonSerializer())
  }

}

internal class HashJsonSerializer: JsonSerializer<Hash>() {
  override fun serialize(value: Hash?, gen: JsonGenerator?, serializers: SerializerProvider?) {
    gen!!.writeString(value!!.toHexString())
  }

}

internal class BlockJsonSerializer : JsonSerializer<Block>() {
  override fun serialize(value: Block?, gen: JsonGenerator?, serializers: SerializerProvider?) {
    if (null == value) {
      gen!!.writeNull()
    } else {
      gen!!.writeStartObject()
      gen.writeStringField("number", value.header().number().toShortHexString())
      gen.writeStringField("hash", value.header().hash().toHexString())
      gen.writeStringField("parentHash", value.header().parentHash()?.toHexString())
      gen.writeStringField("nonce", value.header().nonce()?.toHexString())
      gen.writeStringField("sha3Uncles", value.header().ommersHash().toHexString() ?: null)
      gen.writeStringField("logsBloom", value.header().logsBloom().toHexString())
      gen.writeStringField("transactionsRoot", value.header().transactionsRoot().toHexString())
      gen.writeStringField("stateRoot", value.header().stateRoot().toHexString())
      gen.writeStringField("miner", value.header().coinbase().toHexString())
      gen.writeStringField("difficulty", value.header().difficulty().toHexString())
      gen.writeStringField("totalDifficulty", value.totalDifficulty().toShortHexString())
      gen.writeStringField("extraData", value.header().extraData().toHexString())
      gen.writeStringField("size", "0x00")
      gen.writeStringField("gasLimit", value.header().gasLimit().toBytes().toHexString())
      gen.writeStringField("gasUsed", value.header().gasUsed().toBytes().toHexString())
      gen.writeStringField("timestamp", "0x" + value.header().timestamp().toEpochMilli().toString(16))
      gen.writeArrayFieldStart("transactions")

      var transactionIndex = 0
      value.body().transactions().forEach({
        gen.writeStartObject()
        gen.writeStringField("from", it.sender()?.toHexString())
        gen.writeStringField("gas", it.gasLimit().toBytes().toHexString())
        gen.writeStringField("gasPrice", it.gasPrice().toBytes().toHexString())
        gen.writeStringField("hash", it.hash().toHexString())
        gen.writeStringField("input", it.payload().toHexString())
        gen.writeStringField("nonce", it.nonce().toHexString())
        gen.writeStringField("to", it.to()?.toHexString())
        gen.writeStringField("transactionIndex", transactionIndex.toString(16))
        gen.writeStringField("value", it.value().toHexString())
        val sig = it.signature()
        gen.writeStringField("v", sig.v().toString(16))
        gen.writeStringField("r", sig.r().toString(16))
        gen.writeStringField("s", sig.r().toString(16))
        gen.writeEndObject()
        transactionIndex++
      })
      gen.writeEndArray()

      gen.writeArrayFieldStart("uncles")
      value.body().ommers().forEach {
        gen.writeString(it.hash().toHexString())
      }
      gen.writeEndArray()

      gen.writeEndObject()
    }
  }
}

internal class BlockJsonDeserializer : JsonDeserializer<Block>() {
  override fun deserialize(p: JsonParser?, ctxt: DeserializationContext?): Block {
    val number : UInt256
    val hash : Hash
    val parentHash : Hash
    val nonce : Bytes
    val ommersHash : Hash
    val logsBloom : Bytes
    val transactionsRoot : Hash
    val stateRoot : Hash
    val coinbase : Address
    val difficulty : UInt256
    val totalDifficulty : UInt256
    val extraData : Bytes
    val gasLimit : UInt256
    val gasUsed : UInt256
    val timestamp : Long
    val transactions : List<Transaction>
    val ommers : List<Hash>

    when(p!!.nextFieldName()) {
      "number" -> { number = UInt256.fromHexString(p.nextTextValue()) }
      "hash" -> { hash = Hash.fromHexString(p.nextTextValue()) }
      "parentHash" -> { parentHash = Hash.fromHexString(p.nextTextValue()) }
      "nonce" -> { nonce = Bytes.fromHexString(p.nextTextValue()) }
      "sha3Uncles" -> { ommersHash = Hash.fromHexString(p.nextTextValue()) }
      "logsBloom" -> { logsBloom = Bytes.fromHexString(p.nextTextValue()) }
      "transactionsRoot" -> { transactionsRoot = Hash.fromHexString(p.nextTextValue()) }
      "stateRoot" -> { stateRoot = Hash.fromHexString(p.nextTextValue()) }
      "coinbase" -> { coinbase = Address.fromHexString(p.nextTextValue()) }
      "difficulty" -> { difficulty = UInt256.fromHexString(p.nextTextValue()) }
      "size" -> {}
      "totalDifficulty" -> { totalDifficulty = UInt256.fromHexString(p.nextTextValue()) }
      "extraData" -> { extraData = Bytes.fromHexString(p.nextTextValue()) }
      "gasLimit" -> { gasLimit = UInt256.fromHexString(p.nextTextValue()) }
      "gasUsed" -> { gasUsed = UInt256.fromHexString(p.nextTextValue()) }
      "timestamp" -> {timestamp = p.nextTextValue().toLong(16) }
      "ommers" -> {
        ommers = mutableListOf<Hash>()
        p.nextToken();
        while (p.nextToken() != JsonToken.END_ARRAY) {
          ommers.add(Hash.fromHexString(p.getText()))
        }
      }
      "transactions" -> {
        transactions = mutableListOf<Transaction>()
        p.nextToken();
        while (p.nextToken() != JsonToken.END_ARRAY) {
          transactions.add(Hash.fromHexString(p.getText()))
        }
      }
      else -> { throw IllegalArgumentException("Unsupported key")}
    }
  }


}
