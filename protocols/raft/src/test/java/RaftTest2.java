/*
 *
 *  Licensed to Metamarkets Group Inc. (Metamarkets) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. Metamarkets licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 * /
 *
 */

import io.atomix.cluster.ClusterMetadata;
import io.atomix.cluster.ClusterMetadataService;
import io.atomix.cluster.ClusterService;
import io.atomix.cluster.ManagedClusterMetadataService;
import io.atomix.cluster.ManagedClusterService;
import io.atomix.cluster.Node;
import io.atomix.cluster.NodeId;
import io.atomix.cluster.impl.DefaultClusterMetadataService;
import io.atomix.cluster.impl.DefaultClusterService;
import io.atomix.cluster.messaging.ClusterMessagingService;
import io.atomix.cluster.messaging.ManagedClusterMessagingService;
import io.atomix.cluster.messaging.impl.DefaultClusterMessagingService;
import io.atomix.messaging.Endpoint;
import io.atomix.messaging.ManagedMessagingService;
import io.atomix.messaging.MessagingService;
import io.atomix.messaging.impl.NettyMessagingService;
import io.atomix.primitive.DistributedPrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.primitive.service.AbstractPrimitiveService;
import io.atomix.primitive.service.Commit;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceExecutor;
import io.atomix.primitive.session.Session;
import io.atomix.protocols.raft.RaftClient;
import io.atomix.protocols.raft.RaftProtocol;
import io.atomix.protocols.raft.RaftServer;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.partition.impl.RaftClientCommunicator;
import io.atomix.protocols.raft.partition.impl.RaftNamespaces;
import io.atomix.protocols.raft.partition.impl.RaftServerCommunicator;
import io.atomix.protocols.raft.storage.RaftStorage;
import io.atomix.storage.StorageLevel;
import io.atomix.storage.buffer.BufferInput;
import io.atomix.storage.buffer.BufferOutput;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.Serializer;
import org.junit.Test;

import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 */
public class RaftTest2
{
  private static final OperationId WRITE_OP_ID = OperationId.command("write-op");


  PrimitiveTypeRegistry primitiveTypeRegistry = new PrimitiveTypeRegistry();
  List<Node> memberIds;
  Node currNode = null;

  public RaftTest2() throws Exception
  {
    primitiveTypeRegistry.register(TestPrimitiveType.INSTANCE);

    memberIds = new ArrayList<>(3);
    for (int i = 0; i < 3; i++) {
      memberIds.add(
          Node.builder(NodeId.from("himanshu-member-id-" + i))
              .withType(Node.Type.DATA)
              .withEndpoint(new Endpoint(InetAddress.getLocalHost(), 9001 + i))
              .build()
      );
    }
  }

  @Test
  public void testit() throws Exception
  {

    currNode = memberIds.get(0);
    RaftServer server1 = createServer();

    currNode = memberIds.get(1);
    RaftServer server2 = createServer();

    currNode = memberIds.get(2);
    RaftServer server3 = createServer();

    Thread.sleep(5000);
    currNode = Node.builder(NodeId.from("himanshu-member-id-" + 3))
                   .withType(Node.Type.DATA)
                   .withEndpoint(new Endpoint(InetAddress.getLocalHost(), 9001 + 3))
                   .build();
    RaftClient client = createClient();
    PrimitiveProxy session = createSession(client);
    System.out.println("OP RESULT = " + session.invoke(WRITE_OP_ID, clientSerializer::encode, new WriteOp(5), clientSerializer::decode).get());
    System.err.println(client.getPrimitives(TestPrimitiveType.INSTANCE).get());

//    Thread.sleep(1000*60*60);

  }

  private RaftClient createClient()
  {
    String partitionname = "hraft-test-partition";
    RaftClient client = RaftClient.builder(memberIds.stream().map(node -> node.id()).collect(Collectors.toList()))
                                  .withClientId("hclient-test")
                                  .withNodeId(currNode.id())
                                  .withProtocol(new RaftClientCommunicator(
                                      partitionname,
                                      Serializer.using(RaftNamespaces.RAFT_PROTOCOL),
                                      buildClusterCommunicator()
                                  ))
                                  .build();
    client.connect().join();
    return client;
  }

  private RaftServer createServer() {
    String partitionname = "hraft-test-partition";
    // copied from RaftPartitionServer.buildServer()
    RaftServer server = RaftServer.builder(currNode.id())
                                  .withName(partitionname)
                                  .withProtocol(new RaftServerCommunicator(
                                      partitionname,
                                      clientSerializer,
                                      buildClusterCommunicator()))
                                  .withPrimitiveTypes(primitiveTypeRegistry)
//                                  .withElectionTimeout(Duration.ofMillis(ELECTION_TIMEOUT_MILLIS))
//                                  .withHeartbeatInterval(Duration.ofMillis(HEARTBEAT_INTERVAL_MILLIS))
                                  .withStorage(RaftStorage.builder()
                                                          .withPrefix(partitionname)
                                                          .withStorageLevel(StorageLevel.DISK)
                                                          .withSerializer(Serializer.using(RaftNamespaces.RAFT_STORAGE))
                                                          .withDirectory("/tmp/druid-atomix/")
//                                                          .withMaxSegmentSize(MAX_SEGMENT_SIZE)
                                                          .build())
                                  .build();

    server.bootstrap(memberIds.stream().map(node -> node.id()).collect(Collectors.toList()));
    return server;
  }

  // copied from Atomix.Builder.build()
  private ClusterMessagingService buildClusterCommunicator()
  {
    ManagedMessagingService messagingService = buildMessagingService();
    messagingService.start().join();

    ManagedClusterMetadataService metadataService = buildClusterMetadataService(messagingService);
    metadataService.start().join();

    ManagedClusterService clusterService = buildClusterService(metadataService, messagingService);
    clusterService.start().join();

    ManagedClusterMessagingService clusterMessagingService = buildClusterMessagingService(clusterService, messagingService);
    clusterMessagingService.start().join();
    return clusterMessagingService;
  }

  protected ManagedClusterMessagingService buildClusterMessagingService(
      ClusterService clusterService, MessagingService messagingService) {
    return new DefaultClusterMessagingService(clusterService, messagingService);
  }

  protected ManagedClusterService buildClusterService(ClusterMetadataService metadataService, MessagingService messagingService) {
    return new DefaultClusterService(currNode, metadataService, messagingService);
  }

  protected ManagedClusterMetadataService buildClusterMetadataService(MessagingService messagingService) {
    return new DefaultClusterMetadataService(ClusterMetadata.builder().withBootstrapNodes(memberIds).build(), messagingService);
  }

  protected ManagedMessagingService buildMessagingService() {
    return NettyMessagingService.builder()
                                .withName("hcluster-test")
                                .withEndpoint(currNode.endpoint())
                                .build();
  }

  //copied from RaftTest
  private PrimitiveProxy createSession(RaftClient client) throws Exception {
    return client.newProxy("test", TestPrimitiveType.INSTANCE, RaftProtocol.builder()
                                                                           .withReadConsistency(ReadConsistency.LINEARIZABLE)
                                                                           .withMinTimeout(Duration.ofMillis(250))
                                                                           .withMaxTimeout(Duration.ofSeconds(5))
                                                                           .build())
                 .connect()
                 .get(5, TimeUnit.SECONDS);
  }

  //
  // TestPrimitiveType
  //
  private static class TestPrimitiveType implements PrimitiveType {
    static final TestPrimitiveType INSTANCE = new TestPrimitiveType();

    @Override
    public String id() {
      return "test";
    }

    @Override
    public PrimitiveService newService() {
      return new TestPrimitiveService();
    }

    @Override
    public DistributedPrimitiveBuilder newPrimitiveBuilder(String name, PrimitiveManagementService managementService) {
      throw new UnsupportedOperationException();
    }
  }

  private static final Serializer clientSerializer = Serializer.using(
      KryoNamespace.builder()
                   .register(RaftNamespaces.RAFT_PROTOCOL)
                   .register(WriteOp.class)
                   .build()
  );
  /**
   * Test state machine.
   */
  public static class TestPrimitiveService extends AbstractPrimitiveService
  {
    private Commit<Void> expire;
    private Commit<Void> close;

    @Override
    protected void configure(ServiceExecutor executor) {
      executor.register(WRITE_OP_ID, clientSerializer::decode, this::write, clientSerializer::encode);
//      executor.register(READ, this::read, clientSerializer::encode);
//      executor.register(EVENT, clientSerializer::decode, this::event, clientSerializer::encode);
//      executor.register(CLOSE, c -> close(c));
//      executor.register(EXPIRE, this::expire);
    }

    @Override
    public void onExpire(Session session) {
//      if (expire != null) {
//        expire.session().publish(EXPIRE_EVENT);
//      }
    }

    @Override
    public void onClose(Session session) {
//      if (close != null && !session.equals(close.session())) {
//        close.session().publish(CLOSE_EVENT);
//      }
    }

    @Override
    public void backup(BufferOutput<?> writer) {
//      writer.writeLong(10);
    }

    @Override
    public void restore(BufferInput<?> reader) {
//      assertEquals(10, reader.readLong());
    }

    protected long write(Commit<WriteOp> commit) {
      return commit.value().value;
    }
//
//    protected long read(Commit<Void> commit) {
//      return commit.index();
//    }

//    protected long event(Commit<Boolean> commit) {
//      if (commit.value()) {
//        commit.session().publish(CHANGE_EVENT, clientSerializer::encode, commit.index());
//      } else {
//        for (Session session : sessions()) {
//          session.publish(CHANGE_EVENT, clientSerializer::encode, commit.index());
//        }
//      }
//      return commit.index();
//    }

    public void close(Commit<Void> commit) {
      this.close = commit;
    }

    public void expire(Commit<Void> commit) {
      this.expire = commit;
    }
  }

  private static class WriteOp
  {
    int value;

    public WriteOp(int value)
    {
      this.value = value;
    }
  }
}
