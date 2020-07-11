package org.apache.tuweni.ethclientui

import org.apache.tuweni.ethclient.EthereumClient
import org.apache.tuweni.peer.repository.Identity
import javax.servlet.ServletContext
import javax.ws.rs.GET
import javax.ws.rs.Path
import javax.ws.rs.PathParam
import javax.ws.rs.Produces
import javax.ws.rs.QueryParam
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Response

@Path("identities")
class IdentityService {

  @javax.ws.rs.core.Context
  var context: ServletContext? = null

  @Path("repositories")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  fun get(): List<Map<String, Any>> {
    val client = context!!.getAttribute("ethclient") as EthereumClient
    return client.peerRepositories.keys.sorted()
      .map { mapOf(Pair("name", it), Pair("size", client.peerRepositories[it]!!.size())) }
  }

  @Path("repositories/{key}")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  fun getIdentities(
    @PathParam("key") peerRepositoryId: String,
    @QueryParam("from") from: Int = 0,
    @QueryParam("max") max: Int = 10,
    @QueryParam("ascending") ascending: Boolean = true
  ): Response {
    val client = context!!.getAttribute("ethclient") as EthereumClient
    val peerRepository = client.peerRepositories[peerRepositoryId] ?: return Response.status(404).build()
    return Response.ok(peerRepository.getIdentities(from, max, ascending)).build()
  }
}
