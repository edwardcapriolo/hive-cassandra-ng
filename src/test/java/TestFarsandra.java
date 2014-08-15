  

import io.teknek.farsandra.Farsandra;
import io.teknek.farsandra.LineHandler;
import io.teknek.hiveunit.HiveTestService;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.hadoop.hive.service.HiveServerException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;



public class TestFarsandra extends HiveTestService {

  public TestFarsandra() throws IOException, InterruptedException {
    super();
    fs = new Farsandra();
    fs.withVersion("2.0.4");
    fs.withCleanInstanceOnStart(true);
    fs.withInstanceName("3_1");
    fs.withCreateConfigurationFiles(true);
    fs.withHost("127.0.0.1");
    fs.withSeeds(Arrays.asList("127.0.0.1"));
    fs.withJmxPort(9999);   
    fs.appendLineToYaml("#this means nothing");
    fs.appendLinesToEnv("#this also does nothing");
    fs.withEnvReplacement("#MALLOC_ARENA_MAX=4", "#MALLOC_ARENA_MAX=wombat");
    fs.withYamlReplacement("# NOTE:", "# deNOTE:");
    final CountDownLatch started = new CountDownLatch(1);
    fs.getManager().addOutLineHandler( new LineHandler(){
        @Override
        public void handleLine(String line) {
          System.out.println("out "+line);
          if (line.contains("Listening for thrift clients...")){
            started.countDown();
          }
        }
      } 
    );
    fs.start();
    started.await(10, TimeUnit.SECONDS);
  }


  private Farsandra fs;
  
  @Test
  public void testIt() throws ConnectionException, HiveServerException, TException{
    AstyanaxUtil au = new AstyanaxUtil();
    au.init();
    au.createKeyspace();
    au.cluster.addColumnFamily(au.cluster.makeColumnFamilyDefinition().setName("stats")
            .setDefaultValidationClass("CounterColumnType")
            .setKeyValidationClass("CompositeType(UTF8Type,UTF8Type,UTF8Type,UTF8Type,UTF8Type,UTF8Type,UTF8Type,UTF8Type,UTF8Type,UTF8Type)")
            .setComparatorType("CompositeType(UTF8Type,UTF8Type,UTF8Type,UTF8Type,UTF8Type,UTF8Type,UTF8Type,UTF8Type,UTF8Type,UTF8Type)")
            .setKeyspace("stats"));
    this.client.execute("CREATE TABLE stats (x int) STORED BY 'io.teknek.hive.cassandra.TeknekCassandraHandler' " +
    		"WITH SERDEPROPERTIES ( \"hosts\"=\"localhost\" , \"keyspace\"= \"stats\", \"column_family\"=\"stats\" )");
    this.client.execute("describe stats");
    Assert.assertEquals("", client.fetchAll()  );
  }
  
  @After
  public void tearDown(){
    if (fs != null){
      try {
        fs.getManager().destroyAndWaitForShutdown(6);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }
  
}
