/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tuweni.eth.crawler

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.apache.tuweni.bytes.Bytes
import org.apache.tuweni.concurrent.AsyncResult
import org.apache.tuweni.concurrent.coroutines.asyncResult
import org.apache.tuweni.crypto.SECP256K1
import org.apache.tuweni.devp2p.Endpoint
import org.apache.tuweni.devp2p.EthereumNodeRecord
import org.apache.tuweni.devp2p.Peer
import org.apache.tuweni.devp2p.PeerRepository
import org.apache.tuweni.devp2p.eth.Status
import org.apache.tuweni.devp2p.parseEnodeUri
import org.apache.tuweni.rlpx.wire.WireConnection
import org.slf4j.LoggerFactory
import java.lang.RuntimeException
import java.net.URI
import java.sql.Timestamp
import java.util.UUID
import javax.sql.DataSource
import kotlin.coroutines.CoroutineContext

open class RelationalPeerRepository(
  private val dataSource: DataSource,
  override val coroutineContext: CoroutineContext = Dispatchers.Default,
) : CoroutineScope, PeerRepository {

  companion object {
    internal val logger = LoggerFactory.getLogger(RelationalPeerRepository::class.java)
  }

  private val listeners = mutableListOf<(Peer) -> Unit>()

  override fun addListener(listener: (Peer) -> Unit) {
    listeners.add(listener)
  }

  override suspend fun get(host: String, port: Int, nodeId: SECP256K1.PublicKey): Peer {
    return get(nodeId, Endpoint(host, port))
  }

  fun get(nodeId: SECP256K1.PublicKey, endpoint: Endpoint): Peer {
    dataSource.connection.use { conn ->
      logger.info("Get peer with $nodeId")
      val stmt = conn.prepareStatement("select id,publickey from identity where publickey=?")
      stmt.setBytes(1, nodeId.bytes().toArrayUnsafe())
      try {
        val rs = stmt.executeQuery()
        logger.info("Results")
        rs.use {
          if (!rs.next()) {
            logger.info("Creating new peer with public key ${nodeId.toHexString()}")
            val id = UUID.randomUUID().toString()
            val insert = conn.prepareStatement("insert into identity(id, publickey) values(?, ?)")
            insert.setString(1, id)
            insert.setBytes(2, nodeId.bytes().toArrayUnsafe())
            insert.execute()
            val newPeer = RepositoryPeer(nodeId, id, endpoint, dataSource)
            listeners.let {
              for (listener in listeners) {
                launch {
                  listener(newPeer)
                }
              }
            }
            return newPeer
          } else {
            logger.info("Found existing peer with public key ${nodeId.toHexString()}")
            val id = rs.getString(1)
            val pubKey = rs.getBytes(2)
            return RepositoryPeer(SECP256K1.PublicKey.fromBytes(Bytes.wrap(pubKey)), id, endpoint, dataSource)
          }
        }
      } catch (e: Exception) {
        logger.error(e.message, e)
        throw RuntimeException(e)
      }
    }
  }

  override suspend fun get(uri: URI): Peer {
    val (nodeId, endpoint) = parseEnodeUri(uri)
    return get(nodeId, endpoint)
  }

  override fun getAsync(uri: URI): AsyncResult<Peer> {
    return asyncResult { get(uri) }
  }

  override fun getAsync(uri: String): AsyncResult<Peer> {
    return asyncResult { get(uri) }
  }

  fun recordInfo(wireConnection: WireConnection, status: Status) {
    dataSource.connection.use { conn ->
      val peer = get(wireConnection.peerPublicKey(), Endpoint(wireConnection.peerHost(), wireConnection.peerPort())) as RepositoryPeer
      val stmt =
        conn.prepareStatement(
          "insert into nodeInfo(id, createdAt, host, port, publickey, p2pVersion, clientId, capabilities, genesisHash, bestHash, totalDifficulty, identity) values(?,?,?,?,?,?,?,?,?,?,?,?)"
        )
      stmt.use {
        val peerHello = wireConnection.peerHello!!
        it.setString(1, UUID.randomUUID().toString())
        it.setTimestamp(2, Timestamp(System.currentTimeMillis()))
        it.setString(3, wireConnection.peerHost())
        it.setInt(4, wireConnection.peerPort())
        it.setBytes(5, wireConnection.peerPublicKey().bytesArray())
        it.setInt(6, peerHello.p2pVersion())
        it.setString(7, peerHello.clientId())
        it.setString(8, peerHello.capabilities().map { it.name() + "/" + it.version() }.joinToString(","))
        it.setString(9, status.genesisHash.toHexString())
        it.setString(10, status.bestHash.toHexString())
        it.setString(11, status.totalDifficulty.toHexString())
        it.setString(12, peer.id)

        it.execute()
      }
    }
  }

  internal fun getPeers(infoCollected: Long, from: Int? = null, limit: Int? = null): List<PeerConnectionInfo> {
    dataSource.connection.use { conn ->
      var query = "select distinct nodeinfo.host, nodeinfo.port, nodeinfo.publickey from nodeinfo \n" +
        "  inner join (select id, max(createdAt) as maxCreatedAt from nodeinfo group by id) maxSeen \n" +
        "  on nodeinfo.id = maxSeen.id and nodeinfo.createdAt = maxSeen.maxCreatedAt where createdAt < ? order by createdAt desc"
      if (from != null && limit != null) {
        query += " limit $limit offset $from"
      }
      val stmt =
        conn.prepareStatement(query)
      stmt.use {
        it.setTimestamp(1, Timestamp(infoCollected))
        // map results.
        val rs = stmt.executeQuery()
        val result = mutableListOf<PeerConnectionInfo>()
        while (rs.next()) {
          val pubkey = SECP256K1.PublicKey.fromBytes(Bytes.wrap(rs.getBytes(3)))
          val port = rs.getInt(2)
          val host = rs.getString(1)
          result.add(PeerConnectionInfo(pubkey, host, port))
        }
        return result
      }
    }
  }

  internal fun getPeersWithInfo(infoCollected: Long, from: Int? = null, limit: Int? = null): List<PeerConnectionInfoDetails> {
    dataSource.connection.use { conn ->
      var query = "select distinct nodeinfo.host, nodeinfo.port, nodeinfo.publickey, nodeinfo.p2pversion, nodeinfo.clientId, nodeinfo.capabilities, nodeinfo.genesisHash, nodeinfo.besthash, nodeinfo.totalDifficulty from nodeinfo \n" +
        "  inner join (select id, max(createdAt) as maxCreatedAt from nodeinfo group by id) maxSeen \n" +
        "  on nodeinfo.id = maxSeen.id and nodeinfo.createdAt = maxSeen.maxCreatedAt where createdAt < ?"
      if (from != null && limit != null) {
        query += " limit $limit offset $from"
      }
      val stmt =
        conn.prepareStatement(query)
      stmt.use {
        it.setTimestamp(1, Timestamp(infoCollected))
        // map results.
        val rs = stmt.executeQuery()
        val result = mutableListOf<PeerConnectionInfoDetails>()
        while (rs.next()) {
          val pubkey = SECP256K1.PublicKey.fromBytes(Bytes.wrap(rs.getBytes(3)))
          val port = rs.getInt(2)
          val host = rs.getString(1)
          val p2pVersion = rs.getInt(4)
          val clientId = rs.getString(5)
          val capabilities = rs.getString(6)
          val genesisHash = rs.getString(7)
          val bestHash = rs.getString(8)
          val totalDifficulty = rs.getString(9)
          result.add(PeerConnectionInfoDetails(pubkey, host, port, p2pVersion, clientId, capabilities, genesisHash, bestHash, totalDifficulty))
        }
        return result
      }
    }
  }

  internal fun getPendingPeers(limit: Int): List<PeerConnectionInfo> {
    dataSource.connection.use { conn ->
      val stmt =
        conn.prepareStatement(
          "select distinct endpoint.host, endpoint.port, identity.publickey from endpoint inner " +
            "join identity on (endpoint.identity = identity.id) where endpoint.identity NOT IN (select identity from nodeinfo) order by endpoint.lastSeen desc limit ?"
        )
      stmt.use {
        // map results.
        stmt.setInt(1, limit)
        val rs = stmt.executeQuery()
        val result = mutableListOf<PeerConnectionInfo>()
        while (rs.next()) {
          val pubkey = SECP256K1.PublicKey.fromBytes(Bytes.wrap(rs.getBytes(3)))
          val port = rs.getInt(2)
          val host = rs.getString(1)
          result.add(PeerConnectionInfo(pubkey, host, port))
        }
        return result
      }
    }
  }
}

internal data class PeerConnectionInfo(val nodeId: SECP256K1.PublicKey, val host: String, val port: Int)
internal data class PeerConnectionInfoDetails(val nodeId: SECP256K1.PublicKey, val host: String, val port: Int, val p2pVersion: Int, val clientId: String, val capabilities: String, val genesisHash: String, val bestHash: String, val totalDifficulty: String)

internal class RepositoryPeer(
  override val nodeId: SECP256K1.PublicKey,
  val id: String,
  knownEndpoint: Endpoint,
  private val dataSource: DataSource,
) : Peer {

  init {
    dataSource.connection.use {
      val stmt = it.prepareStatement("select lastSeen,lastVerified,host,port from endpoint where identity=?")
      stmt.use {
        it.setString(1, id)
        val rs = it.executeQuery()
        if (rs.next()) {
          val lastSeenStored = rs.getTimestamp(1)
          val lastVerifiedStored = rs.getTimestamp(2)
          val host = rs.getString(3)
          val port = rs.getInt(4)
          if (knownEndpoint.address == host && knownEndpoint.udpPort == port) {
            lastSeen = lastSeenStored.time
            lastVerified = lastVerifiedStored.time
          }
        }
      }
    }
  }

  @Volatile
  override var endpoint: Endpoint = knownEndpoint

  override var enr: EthereumNodeRecord? = null

  @Synchronized
  override fun getEndpoint(ifVerifiedOnOrAfter: Long): Endpoint? {
    if ((lastVerified ?: 0) >= ifVerifiedOnOrAfter) {
      return this.endpoint
    }
    return null
  }

  @Volatile
  override var lastVerified: Long? = null

  @Volatile
  override var lastSeen: Long? = null

  @Synchronized
  override fun updateEndpoint(endpoint: Endpoint, time: Long, ifVerifiedBefore: Long?): Endpoint {
    val currentEndpoint = this.endpoint
    if (currentEndpoint == endpoint) {
      this.seenAt(time)
      return currentEndpoint
    }

    if (ifVerifiedBefore == null || (lastVerified ?: 0) < ifVerifiedBefore) {
      if (currentEndpoint.address != endpoint.address || currentEndpoint.udpPort != endpoint.udpPort) {
        lastVerified = null
      }
      this.endpoint = endpoint
      this.seenAt(time)
      return endpoint
    }

    return currentEndpoint
  }

  @Synchronized
  override fun verifyEndpoint(endpoint: Endpoint, time: Long): Boolean {
    if (endpoint != this.endpoint) {
      return false
    }
    if ((lastVerified ?: 0) < time) {
      lastVerified = time
    }
    seenAt(time)
    return true
  }

  @Synchronized
  override fun seenAt(time: Long) {
    if ((lastSeen ?: 0) < time) {
      lastSeen = time
      persist()
    }
  }

  @Synchronized
  override fun updateENR(record: EthereumNodeRecord, time: Long) {
    if (enr == null || enr!!.seq() < record.seq()) {
      enr = record
      updateEndpoint(Endpoint(record.ip().hostAddress, record.udp()!!, record.tcp()), time)
    }
  }

  fun persist() {
    dataSource.connection.use { conn ->
      val stmt =
        conn.prepareStatement(
          "insert into endpoint(id, lastSeen, lastVerified, host, port, identity) values(?,?,?,?,?,?)"
        )
      stmt.use {
        it.setString(1, UUID.randomUUID().toString())
        it.setTimestamp(2, Timestamp(lastSeen ?: 0))
        it.setTimestamp(3, Timestamp(lastVerified ?: 0))
        it.setString(4, endpoint.address)
        it.setInt(5, endpoint.udpPort)
        it.setString(6, id)
        it.execute()
      }
    }
  }
}
