package org.apache.tuweni.jsonrpc

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import io.netty.handler.codec.http.HttpRequest
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpServer
import io.vertx.core.http.HttpServerRequest
import io.vertx.core.net.NetServer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.apache.tuweni.bytes.Bytes32
import org.apache.tuweni.concurrent.AsyncCompletion
import org.apache.tuweni.concurrent.coroutines.await
import org.apache.tuweni.eth.repository.BlockchainRepository
import org.apache.tuweni.units.bigints.UInt256
import java.lang.IllegalArgumentException
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.CoroutineContext

/**
 * A JSON-RPC server implementing the [Ethereum JSON-RPC specification](https://github.com/ethereum/wiki/wiki/JSON-RPC).
 *
 * @param vertx the Vert.x instance to build network resources
 * @param networkInterface the network interface to bind the server to
 * @param port the port to bind the server to
 * @param coroutineContext the context to use to run coroutines in
 */
class JSONRPCServer(private val vertx : Vertx, private val networkInterface : String = "127.0.0.1", private val port : Int,
                    private val repository : BlockchainRepository,
                    override val coroutineContext: CoroutineContext = Dispatchers.Default
): CoroutineScope {

  companion object {
    private val objectMapper = ObjectMapper().registerModule(JacksonJSONRPCModule())
  }

  private val started = AtomicBoolean(false)
  private var server: HttpServer? = null

  init {
    if (port < 0 || port > 65535) {
      throw IllegalArgumentException("Invalid port")
    }
  }

  /**
   * Starts the server.
   */
  suspend fun start() {
    if (started.compareAndSet(false, true)) {
        val asyncCompletion = AsyncCompletion.incomplete()

        server = vertx.createHttpServer().requestHandler(this::handleRequest).listen(port, networkInterface, {
          if (it.failed()) {
            asyncCompletion.completeExceptionally(it.cause())
          } else {
            asyncCompletion.complete()
          }
        })
        asyncCompletion.await()
    }
  }

  private fun handleRequest(request : HttpServerRequest) {
    request.bodyHandler {
      runBlocking {
        val body = objectMapper.readTree(it.bytes)
        val method = body.get("method").asText()
        when (method) {
          "eth_blockNumber" -> {
            request.response().statusCode = 200
            val blockNumber = repository.retrieveChainHeadHeader()?.number()?.toShortHexString()?.substring(2) ?: ""
            request.response().end(Buffer.buffer(createResponse(body.get("id").asInt(), blockNumber)))
          }
          "eth_getBlockByNumber" -> {
            request.response().statusCode = 200
            val number = Bytes32.fromHexString((body.get("params") as ArrayNode).get(0).asText())
            val hashes = repository.findBlockByHashOrNumber(number)
            val result = hashes.get(0).let {
              repository.retrieveBlock(it)
            }
            result?.header()?.hash()?.let {
              repository.totalDifficulty(it)?.let {
                result.setTotalDifficulty(it)
              }
            }

            request.response().end(Buffer.buffer(createResponse(body.get("id").asInt(), result)))
          }
          else -> {
            request.response().statusCode = 503
            request.response().end(method + " is not supported")
          }
        }
      }
    }
  }

  private fun createResponse(id : Int, result : Any?) : ByteArray {
    val map = mapOf<String, Any?>(Pair("id", id), Pair("jsonrpc", "2.0"), Pair("result", result))
    return objectMapper.writeValueAsBytes(map)
  }

  /**
   * Provides the port used by the server.
   * @return the port the server uses for communications
   * @throws IllegalStateException if the server was not started prior
   */
  fun port() : Int {
    if (!started.get()) {
      throw IllegalStateException("Server not started")
    }
    return server!!.actualPort()
  }

  /**
   * Stops the server.
   */
  suspend fun stop() {
    if (started.compareAndSet(true,false)) {
      val asyncCompletion = AsyncCompletion.incomplete()
      server!!.close({
        if (it.failed()) {
          asyncCompletion.completeExceptionally(it.cause())
        } else {
          asyncCompletion.complete()
        }
      })
      asyncCompletion.await()
    }
  }
}
