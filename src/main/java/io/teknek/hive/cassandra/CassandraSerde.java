package io.teknek.hive.cassandra;

import io.teknek.cassandra.simple.FramedConnWrapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Writable;
import org.apache.thrift.TException;

public class CassandraSerde implements SerDe {

  private String KEYSPACE = "keyspace";
  private String COLUMN_FAMILY = "column_family";
  private String HOSTS = "hosts";
  private String PORT ="port";
  private ObjectInspector cachedInspector;
  
  @Override
  public Object deserialize(Writable arg0) throws SerDeException {
    
    return null;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return cachedInspector;
  }

  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }

  @Override
  public void initialize(Configuration configuration, Properties tbl) throws SerDeException {
    String keyspace = tbl.getProperty(KEYSPACE);
    String columnFamily = tbl.getProperty(COLUMN_FAMILY);
    FramedConnWrapper w = new FramedConnWrapper(tbl.getProperty(HOSTS),9160);
    KsDef ks;
    Cassandra.Client c;
    try {
      w.open();
      c = w.getClient();
      ks = c.describe_keyspace(keyspace);
    } catch (Exception e) {
      throw new SerDeException(e);
    }
    CfDef d = null;
    for (CfDef it : ks.cf_defs){
      if (it.name.equalsIgnoreCase(columnFamily)){
        d = it;
      }
    }
    if (d == null){
      throw new SerDeException("could not find column family " + d);
    }
    /* We have column metadata treat columns as columns
     * Put ownknown columns in map<Comparator,DefaultValidator>
     */
    if (d.getColumn_metadata().size() > 0){
      List<ObjectInspector> ois = new ArrayList<ObjectInspector>();
      List<String> columnNames = new ArrayList<String>();
      columnNames.add("rowkey");
      ois.add(parseInspectorString(d.key_validation_class, "rowkey"));
      cachedInspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, ois);
    } else {
      List<String> columns = Arrays.asList("rowkey", "column", "value", "timestamp");
      List<ObjectInspector> ois = Arrays.asList(
            parseInspectorString(d.key_validation_class,"rowkey"),
            parseInspectorString(d.getComparator_type(), "column"),
            parseInspectorString(d.getDefault_validation_class(), "value"),
            PrimitiveObjectInspectorFactory.javaLongObjectInspector
            );
      cachedInspector = ObjectInspectorFactory.getStandardStructObjectInspector(columns, ois);
    }
    //
    //String validationClass = ;
    
  }

  public ObjectInspector parseInspectorString(String value, String base){
    
    if (value.startsWith("org.apache.cassandra.db.marshal.CompositeType(")){
      List<String> columnNames = new ArrayList<String>();
      List<ObjectInspector> values = new ArrayList<ObjectInspector>();
      String [] parts = value.substring(value.indexOf("(")+1, value.length()-1).split(",");
      for (int i = 0 ;i<parts.length;i++){
        columnNames.add(base+i);
        values.add( parseInspectorString(parts[i],""));
      }
      return ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, values);
    } else if (value.equals("org.apache.cassandra.db.marshal.UTF8Type")){
      return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    } else if (value.equals("org.apache.cassandra.db.marshal.BytesType")){
      return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
              (PrimitiveTypeInfo) TypeInfoFactory.binaryTypeInfo);
    } else if (value.equals("org.apache.cassandra.db.marshal.CounterColumnType")){
      return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
    }
    else {
      throw new IllegalArgumentException("Do not know what to do with "+ value);
    }
    
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Writable serialize(Object arg0, ObjectInspector arg1) throws SerDeException {
    // TODO Auto-generated method stub
    return null;
  }
}

 
