package io.teknek.hive.cassandra;

import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.serde2.SerDe;

public class TeknekCassandraHandler extends DefaultStorageHandler {

  @Override
  public Class<? extends SerDe> getSerDeClass() {
    return CassandraSerde.class;
  }
  
  
}
