package com.redislabs;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import gears.ExecutionMode;
import gears.GearsBuilder;
import gears.GearsFuture;
import gears.LogLevel;
import gears.readers.KeysReader;
import gears.records.KeysReaderRecord;

public class WriteSource extends Source{
  
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private String streamName;
  private int timeout;
  
  private boolean writeThrough;
  boolean writeOnChange;
  
  public WriteSource() {
    super();
  }
  
  public WriteSource(String name, String connector, String xmlDef, boolean writeThrough, int timeout, boolean writeOnChange) {
    super(connector, name, xmlDef);
    this.writeThrough = writeThrough;
    this.timeout = timeout;
    this.writeOnChange = writeOnChange;
    
    Connector c = getConnectorObj();
    
    this.streamName = String.format("_Stream-%s-%s", connector, c.getUuid());
   
    KeysReader reader = new KeysReader().
        setPattern(getHashPrefix() + ":*");
    if ( writeOnChange )
        reader.setEventTypes(new String[] {"hset", "hmset", "hincrbyfloat", "hincrby", "hdel", "del", "change"});
    else
        reader.setEventTypes(new String[] {"hset", "hmset", "hincrbyfloat", "hincrby", "hdel", "del"});

    if(writeThrough) {
      /*
       *  on write through we will set a command filter to intercept the command,
       *  although we will not touch the return reply, this will block the client
       *  until we finish write the data to the backend.
       */
      reader.setCommands(new String[] {"hset", "hmset", "hincrbyfloat", "hincrby", "hdel", "del", "hsetnx"});
      
      GearsBuilder<KeysReaderRecord> builder = GearsBuilder.CreateGearsBuilder(reader, "write registration for source " + this.getName());
      builder.asyncMap(r->{
        try {
          return this.asyncforeach(r);
        } catch (Exception e) {
          GearsBuilder.log(String.format("Error: ", e));
          return null;
        }
      }).
      filter(r->r.startsWith("-ERR")).
      foreach(r->GearsBuilder.overrideReply(r)).
      register(ExecutionMode.ASYNC_LOCAL, this, this);
      
      // register another registration that will handle change events from another replica
      // on crdt
      reader = new KeysReader().
          setPattern(getHashPrefix() + ":*").
          setEventTypes(new String[] {"change"});
      
      builder = GearsBuilder.CreateGearsBuilder(reader, "keys reader (crdt support) for source " + this.getName());
      builder.foreach(r->this.foreach(r)).
      register(ExecutionMode.SYNC, this, this);
      
    } else {
        
      GearsBuilder<KeysReaderRecord> builder = GearsBuilder.CreateGearsBuilder(reader, "keys reader for source " + this.getName());
      builder.foreach(r->this.foreach(r)).
      register(ExecutionMode.SYNC, this, this);
      
    }
    
  }

  public GearsFuture<String> asyncforeach(KeysReaderRecord record) throws Exception {
    GearsFuture<String> f = null;
    f = new GearsFuture<>();
    
    String streamId = null;
    
    /*
     * We must take the redis GIL here,
     * Writing to the steam and to the connector queue
     * must be atomic otherwise we might have a race condition
     * where the connector will finish the write
     * before we wrote the future object to the connector queue
     * and the client will not be released.
     */
    GearsBuilder.acquireRedisGil();
    try {
      
      streamId = foreachInternal(record);
      
      WriteThroughMD wtMD = new WriteThroughMD(streamId, f, timeout * 1000L);
      Connector c = Connector.getConnector(this.getConnector());
      c.queue.add(wtMD);
    }catch(Exception e) {
      GearsBuilder.overrideReply(String.format("-Err %s", e));
      f.setError(e.toString());
    }finally {
      GearsBuilder.releaseRedisGil();
    }
    return f;
  }
  
  public void foreach(KeysReaderRecord record) throws Exception {
    try {
      foreachInternal(record);
    } catch(Exception e) {
      GearsBuilder.log(String.format("-Err %s", e));
      throw e;
    }
  }
  
  public String foreachInternal(KeysReaderRecord record) throws Exception {
    String key = record.getKey();
    String[] keySplit = key.split(":");
    
    String id = keySplit[1];
    // make sure id has the correct type
    try {
      getIdProperty().convertToObject(id);
    }catch (Exception e) {
      String msg = String.format("Failed parsing id field \"%s\", value \"%s\", error=\"%s\"", getIdProperty().getName(), id, e);
      GearsBuilder.log(msg, LogLevel.WARNING);
      throw new Exception(msg);
    }
    
    String[] command;
    Stream<String> commandStreamInit = Stream.of("XADD", String.format("%s-{%s}", streamName, GearsBuilder.hashtag()), "*", 
                                             Connector.ENTETY_NAME_STR, keySplit[0], getIdProperty().getName(), keySplit[1], 
                                             Connector.SOURCE_STR, this.getName());
    
    Map<String, String> value = record.getHashVal();
    // null value here will be considered as delete
    if(value != null) {
      Stream<String> commandStream = Stream.concat(commandStreamInit, Stream.of(Connector.EVENT_STR, "hset"));
      
      // verify schema:
      Map<String, String> valuesToWrite = new HashMap<>();
      for(PropertyData pd : getPropertyMappings().values()) {
        String val = null;
        try {
          val = value.get(pd.getName());
          if(val != null) {
            pd.convertToObject(val);
            valuesToWrite.put(pd.getName(), val);
          }else if(!pd.isNullable()) {
            throw new Exception(String.format("mandatory \"%s\" value is not set", pd.getName()));
          }
        }catch(Exception e) {
          String msg = String.format("Failed parsing acheme for field \"%s\", value \"%s\", error=\"%s\"", pd.getName(), val, e);
          GearsBuilder.log(msg, LogLevel.WARNING);
          throw new Exception(msg);
        }
      }      
      
      Stream<String> fieldsStream = valuesToWrite.entrySet().stream()
          .flatMap(entry -> Stream.of(entry.getKey(), entry.getValue()));

      command = Stream.concat(commandStream, fieldsStream).toArray(String[]::new);
    }else {
      Stream<String> commandStream = Stream.concat(commandStreamInit, Stream.of(Connector.EVENT_STR, "del"));
      command = commandStream.toArray(String[]::new);
    }
  
    return (String)GearsBuilder.execute(command); // Write to stream
  }
  
  @Override
  public String toString() {
    return String.format("name: %s, connector: %s, xmlDef: %s, idName: %s", this.getName(), this.getConnector(), this.getXmlDef(), getIdProperty().getName());
  }
  
  @Override
  public Iterator<Object> iterator() {
    Iterator<Object> sup = super.iterator();
    Iterable<Object> iterable = () -> sup;
    List<Object> s = StreamSupport.stream(iterable.spliterator(), false).collect(Collectors.toList());
    s.add("steamName");
    s.add(String.format("%s-{%s}", streamName, GearsBuilder.hashtag()));
    s.add("policy");
    s.add(writeThrough ? "writeThrough" : "writeBehind");
    s.add("writeOnChangeEvent");
    s.add(Boolean.toString(writeOnChange));
    if(writeThrough) {
      s.add("timeout");
      s.add(Integer.toString(timeout));
    }
    
    return s.iterator();
  }

  public String getStreamName() {
    return streamName;
  }

  public boolean isWriteThrough() {
    return writeThrough;
  }

  public int getTimeout() {
    return timeout;
  }
  public boolean getWriteOnChange() {
    return writeOnChange;
  }

  public void setTimeout(int timeout) {
    this.timeout = timeout;
  }

  public void setStreamName(String streamName) {
    this.streamName = streamName;
  }

  public void setWriteThrough(boolean writeThrough) {
    this.writeThrough = writeThrough;
  }

  public void setWriteOnChange(boolean writeOnChange) {
    this.writeOnChange = writeOnChange;
  }
}
