package com.redislabs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import gears.ExecutionMode;
import gears.GearsBuilder;
import gears.operations.ForeachOperation;
import gears.readers.KeysReader;
import gears.records.KeysReaderRecord;

public class ReadSource extends Source implements ForeachOperation<KeysReaderRecord>{

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  private int expire = 0;
  
  public ReadSource() {}
  
  public ReadSource(String name, String connector, String xmlDef, int expire) {
    super(connector, name, xmlDef);
    
    this.expire = expire;
    
    KeysReader reader = new KeysReader().setPattern(getHashPrefix() + ":*").
        setEventTypes(new String[] {"keymiss"}).
        setCommands(new String[] {"hget", "hmget", "hgetall"});
    
    GearsBuilder.CreateGearsBuilder(reader, "read registration for source " + this.getName()).
    foreach(this).
    register(ExecutionMode.ASYNC_LOCAL, this, this);
  }
  
  @Override
  public void foreach(KeysReaderRecord r) throws Exception {
    String key = r.getKey();
    String pkStr = key.split(":")[1];
    Object pk = getIdProperty().convert(pkStr);
    byte[][] originalCommand = GearsBuilder.getCommand();
    String originalCmd = new String(originalCommand[0]).toLowerCase();
    List<String> fields = null;
    if(originalCmd.equals("hget") || originalCmd.equals("hmget")) {
      fields = new ArrayList<>();
      for(int i = 2 ; i < originalCommand.length ; ++i) {
        fields.add(new String(originalCommand[i]).toLowerCase());
      }
    }
    Map<String, Object> res = null;
    try {
      res = (Map<String, Object>)getConnectorObj().getObject(getHashPrefix(), (Serializable)pk);
    } catch (Exception e) {
      GearsBuilder.overrideReply(String.format("-ERR %s", e.toString()));
      throw e;
    }
    if (res != null) {
      List<String> command = new ArrayList<>();
      command.add("hset");
      command.add(key);
      for(Entry<String, Object> e: res.entrySet()) {
        if(e.getKey().startsWith("$")) {
          continue;
        }
        command.add(e.getKey());
        command.add(e.getValue().toString());
      }
      
      
      try {
        GearsBuilder.acquireRedisGil();
        float memoryRatio = 0;
        try {
          memoryRatio = GearsBuilder.getMemoryRatio();
        }catch (Exception e) {
          // if we reached here it means that getMemoryRatio is not supported and we will not handle it
        }
        if(memoryRatio >= 1) {
          // we reached max memory and we can not write the data, we will reply with OOM error.
          GearsBuilder.overrideReply("-OOM command not allowed when used memory > 'maxmemory'");
          throw new Exception("OOM Reached");
        }
        boolean old = GearsBuilder.setAvoidNotifications(true); 
        GearsBuilder.executeArray(command.toArray(new String[0]));
        if(this.expire > 0) {
          GearsBuilder.execute("expire", key, Integer.toString(this.expire));        
        }
        GearsBuilder.setAvoidNotifications(old);
      }finally {
        GearsBuilder.releaseRedisGil();
      }
      
      
      List<String> response = new ArrayList<>();
      if(fields == null) {
        for(Entry<String, Object> e: res.entrySet()) {
          if(e.getKey().startsWith("$")) {
            continue;
          }
          response.add(e.getKey());
          response.add(e.getValue().toString());
        }
      }else {
        for(String f : fields) {
          if(res.containsKey(f)) {
            response.add(res.get(f).toString());
          }else {
            response.add(null);
          }
        }
      }
      
      if(response.isEmpty() && originalCmd.equals("hget")) {
        GearsBuilder.overrideReply(response.get(0));
      }else {
        GearsBuilder.overrideReply(response);
      }
    }
  }
  
  @Override
  public Iterator<Object> iterator() {
    Iterator<Object> sup = super.iterator();
    Iterable<Object> iterable = () -> sup;
    List<Object> s = StreamSupport.stream(iterable.spliterator(), false).collect(Collectors.toList());
    s.add("policy");
    s.add("readThrough");
    s.add("expire");
    s.add(Integer.toString(expire));
    
    return s.iterator();
  }

  public int getExpire() {
    return expire;
  }

  public void setExpire(int expire) {
    this.expire = expire;
  }

}
