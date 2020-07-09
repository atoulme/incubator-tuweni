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
package org.apache.tuweni.peer.repository.jpa

import org.apache.tuweni.crypto.SECP256K1
import org.apache.tuweni.junit.BouncyCastleExtension
import org.apache.tuweni.peer.repository.memory.MemoryPeerRepository
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import java.time.Instant
import java.time.temporal.ChronoUnit
import javax.persistence.Persistence

@ExtendWith(BouncyCastleExtension::class)
class JPAPeerRepositoryTest {

  val entityManagerFactory = Persistence.createEntityManagerFactory("h2")
  @Test
  fun testEmptyByDefault() {
    val repo = JPAPeerRepository(entityManagerFactory)
    Assertions.assertNull(repo.randomPeer())
  }

  @Test
  fun testStorePeerAndIdentity() {
    val repo = JPAPeerRepository(entityManagerFactory)
    val identity = repo.storeIdentity("0.0.0.0", 12345, SECP256K1.KeyPair.random().publicKey())
    val peer = repo.storePeer(identity, Instant.now(), Instant.now())
    val conn = repo.addConnection(peer, identity)
    repo.peerDiscoveredAt(peer, Instant.now().toEpochMilli())
    Assertions.assertEquals(1, conn.identity().connections().size)
    Assertions.assertEquals(1, conn.peer().connections().size)
    Assertions.assertEquals(1, conn.identity().activePeers().size)
    Assertions.assertEquals(peer, conn.identity().activePeers().get(0))

    repo.markConnectionInactive(peer, identity)
    Assertions.assertEquals(0, identity.activePeers().size)
  }

  @Test
  fun testLastContacted() {
    val repo = JPAPeerRepository(entityManagerFactory)
    val identity = repo.storeIdentity("0.0.0.0", 12345, SECP256K1.KeyPair.random().publicKey())
    val fiveSecondsAgo = Instant.now().minus(5, ChronoUnit.SECONDS)
    val peer = repo.storePeer(identity, fiveSecondsAgo, Instant.now())
    repo.addConnection(peer, identity)
    Assertions.assertTrue(peer.lastContacted()!!.isAfter(fiveSecondsAgo))
  }
}
