

import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class AstyanaxUtil { 

  public AstyanaxContext<Keyspace> context;
  public Keyspace keyspace;
  public Cluster cluster;
  
  public void init(){
    AstyanaxContext<Cluster> clusterContext = new AstyanaxContext.Builder()
    .forCluster("127.0.0.1:9160")
    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl())
    .withConnectionPoolConfiguration(
            new ConnectionPoolConfigurationImpl("ClusterName").setMaxConnsPerHost(1)
                    .setSeeds("127.0.0.1:9160"))
    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
    .buildCluster(ThriftFamilyFactory.getInstance());
    clusterContext.start();
    cluster = clusterContext.getEntity();

    context = new AstyanaxContext.Builder()
    .forCluster("ClusterName")
    .forKeyspace("stats")
    .withAstyanaxConfiguration(
            new AstyanaxConfigurationImpl()
                    .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE))
    .withConnectionPoolConfiguration(
            new ConnectionPoolConfigurationImpl("MyConnectionPool").setPort(9160)
                    .setMaxConnsPerHost(1).setSeeds("127.0.0.1:9160"))
    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
    .buildKeyspace(ThriftFamilyFactory.getInstance());
    context.start();
    keyspace = context.getEntity();
  }
  
  public void createKeyspace() throws ConnectionException{
    keyspace.createKeyspace(ImmutableMap
            .<String, Object> builder()
            .put("strategy_options",
                    ImmutableMap.<String, Object> builder().put("replication_factor", "1")
                            .build()).put("strategy_class", "SimpleStrategy").build());
  }
}
