package com.redislabs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Stream;
import java.io.Serializable;

import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.Property;

import com.fasterxml.jackson.annotation.JsonIgnore;

import gears.ExecutionMode;
import gears.GearsBuilder;
import gears.GearsFuture;
import gears.LogLevel;
import gears.operations.AsyncMapOperation;
import gears.operations.ForeachOperation;
import gears.operations.GearsFutureOnDone;
import gears.operations.OnRegisteredOperation;
import gears.operations.OnUnregisteredOperation;
import gears.readers.CommandOverrider;
import gears.readers.KeysReader;
import gears.records.KeysReaderRecord;
import oracle.ucp.common.waitfreepool.Tuple;


//Object pendingsWrites = GearsBuilder.execute("xlen", String.format("%s-{%s}", streamName, GearsBuilder.hashtag()));
//s.add("PendingWrites");
//s.add(pendingsWrites.toString());


public class Source implements ForeachOperation<KeysReaderRecord>,
  OnRegisteredOperation,
  OnUnregisteredOperation,
  Iterable<Object>{

  public static ConcurrentLinkedDeque<Tuple<String, GearsFuture<Serializable>>> queue = new ConcurrentLinkedDeque<>();  
  private static boolean writeThroughIsRegistered = false;
  
  
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private String hashPrefix;
  private String name;
  private String connector;
  private String xmlDef;
  private String streamName;
  private String registrationId;
  
  @JsonIgnore
  private PropertyData idProperty;
  
  @JsonIgnore
  private HashMap<String, PropertyData> propertyMappings;
  
  private boolean writeThrough;
  
  private static ThreadLocal<List<GearsFuture<Serializable>>> threadLocal = new ThreadLocal<>();;
  
  private static Map<String, Source> sources = new ConcurrentHashMap<>();
  
  public static Source getSource(String name) {
    return sources.get(name);    
  }
  
  public static Collection<Source> getAllSources() {
    return sources.values();    
  }
  
  public Source() {}
  
  public Source(String name, String connector, String xmlDef, boolean writeThrough) {
    this.name = name;
    this.connector = connector;
    this.xmlDef = xmlDef;
    this.propertyMappings = new HashMap<String, PropertyData>();
    this.writeThrough = writeThrough;
    
    Connector c = Connector.GetConnector(connector);
    
    this.streamName = String.format("_Stream-%s-%s", connector, c.getUuid());
    
    StandardServiceRegistry tempRegistry = new StandardServiceRegistryBuilder()
        .configure( InMemoryURLFactory.getInstance().build("configuration", RGHibernate.get(connector).getXmlConf()))
        .build();
    
    MetadataSources tempSources = new MetadataSources(tempRegistry);
    tempSources.addURL(InMemoryURLFactory.getInstance().build("mapping", xmlDef));
    Metadata metadata = tempSources.getMetadataBuilder().build();
    PersistentClass pc = metadata.getEntityBindings().iterator().next();
    hashPrefix = pc.getEntityName();
    Property idProp = pc.getIdentifierProperty();
    idProperty =  new PropertyData(idProp.getName(), 
        ((Column)idProp.getColumnIterator().next()).getName(), 
        idProp.getType(), idProp.isOptional(), true);
    Iterator<Property> iter = pc.getPropertyIterator();
    while(iter.hasNext()) {
      Property p = iter.next();
      PropertyData pd = new PropertyData(p.getName(), 
          ((Column)p.getColumnIterator().next()).getName(), 
          p.getType(), p.isOptional(), false);
      propertyMappings.put(pd.getName(), pd);
    }
    
    tempRegistry.close();
   
    KeysReader reader = new KeysReader().
        setPattern(hashPrefix + ":*").
        setEventTypes(new String[] {"hset", "hmset", "hincrbyfloat", "hincrby", "hdel", "del", "changed"});
        
    GearsBuilder<KeysReaderRecord> builder = GearsBuilder.CreateGearsBuilder(reader, "keys reader for source " + this.name);
    builder.foreach(this).register(ExecutionMode.SYNC, this, this);
    
    sources.put(name, this);
    
    if(writeThrough && !writeThroughIsRegistered) {
      AsyncMapOperation<Object[], Serializable> asyncMapper = 
          new  AsyncMapOperation<Object[], Serializable>() {
            /**
             * 
             */
            private static final long serialVersionUID = 1L;

            @Override
            public GearsFuture<Serializable> map(Object[] r) {
              List<String> args = new ArrayList<String>();
              for(int i = 1 ; i < r.length ; ++i) {
                args.add(new String((byte[])r[i]));
              }
              Object res = GearsBuilder.callNextArray(args.toArray(new String[0]));
              List<GearsFuture<Serializable>> l = threadLocal.get();
              threadLocal.set(null);
              GearsFuture<Serializable> f = new GearsFuture<Serializable>();
              if(l == null) {
                try {
                  f.setResult((Serializable)res);
                } catch (Exception e) {
                  GearsBuilder.log(e.toString(), LogLevel.WARNING);
                  return null;
                }
              }else {
                GearsFutureOnDone<Serializable> onDone = new GearsFutureOnDone<Serializable>() {
                  
                  int expectedResults = l.size();
                  int actualResults = 0;
                  String error;
                  
                  private void ReturnResult() throws Exception {
                    if(this.error != null) {
                      f.setError(this.error);
                    }else {
                      f.setResult((Serializable)res);
                    }
                  }
                  
                  @Override
                  public void OnFailed(String error) throws Exception {
                    this.error = error;
                    actualResults++;
                    if(actualResults == expectedResults) {
                      ReturnResult();
                    }
                  }
                  
                  @Override
                  public void OnDone(Serializable record) throws Exception {
                    actualResults++;
                    if(actualResults == expectedResults) {
                      ReturnResult();
                    }
                  }
                };
                for(GearsFuture<Serializable> future : l) {
                  try {
                    future.setFutureCallbacks(onDone);
                  } catch (Exception e) {
                    GearsBuilder.log(e.toString(), LogLevel.WARNING);
                    return null;
                  }
                }
              }
              return f;
            }
      };
      
      CommandOverrider hsetco = new CommandOverrider().setCommand("hset");
      GearsBuilder.CreateGearsBuilder(hsetco, "hset command override").
      asyncMap(asyncMapper).register(ExecutionMode.ASYNC_LOCAL, 
          (id)->{writeThroughIsRegistered=true;}, ()->{writeThroughIsRegistered=false;});
      
      CommandOverrider delco = new CommandOverrider().setCommand("del");
      GearsBuilder.CreateGearsBuilder(delco, "del command override").
      asyncMap(asyncMapper).register(ExecutionMode.ASYNC_LOCAL, 
          (id)->{writeThroughIsRegistered=true;}, ()->{writeThroughIsRegistered=false;});
      
      CommandOverrider hmsetco = new CommandOverrider().setCommand("hmset");
      GearsBuilder.CreateGearsBuilder(hmsetco, "hmsetco command override").
      asyncMap(asyncMapper).register(ExecutionMode.ASYNC_LOCAL, 
          (id)->{writeThroughIsRegistered=true;}, ()->{writeThroughIsRegistered=false;});
      
      CommandOverrider hincrbyfloatco = new CommandOverrider().setCommand("hincrbyfloat");
      GearsBuilder.CreateGearsBuilder(hincrbyfloatco, "hmsetco command override").
      asyncMap(asyncMapper).register(ExecutionMode.ASYNC_LOCAL, 
          (id)->{writeThroughIsRegistered=true;}, ()->{writeThroughIsRegistered=false;});
      
      CommandOverrider hincrbyco = new CommandOverrider().setCommand("hincrby");
      GearsBuilder.CreateGearsBuilder(hincrbyco, "hmsetco command override").
      asyncMap(asyncMapper).register(ExecutionMode.ASYNC_LOCAL, 
          (id)->{writeThroughIsRegistered=true;}, ()->{writeThroughIsRegistered=false;});
      
      CommandOverrider hdelco = new CommandOverrider().setCommand("hdel");
      GearsBuilder.CreateGearsBuilder(hdelco, "hmsetco command override").
      asyncMap(asyncMapper).register(ExecutionMode.ASYNC_LOCAL, 
          (id)->{writeThroughIsRegistered=true;}, ()->{writeThroughIsRegistered=false;});
      
      CommandOverrider hsetnxco = new CommandOverrider().setCommand("hsetnx");
      GearsBuilder.CreateGearsBuilder(hsetnxco, "hmsetco command override").
      asyncMap(asyncMapper).register(ExecutionMode.ASYNC_LOCAL, 
          (id)->{writeThroughIsRegistered=true;}, ()->{writeThroughIsRegistered=false;});
    }
    
  }
  
  public String getName() {
    return name;
  }

  public String getConnector() {
    return connector;
  }

  public String getXmlDef() {
    return xmlDef;
  }

  @Override
  public void foreach(KeysReaderRecord record) throws Exception {
    String key = record.getKey();
    String[] keySplit = key.split(":");
    
    String id = keySplit[1];
    // make sure id has the correct type
    try {
      idProperty.convert(id);
    }catch (Exception e) {
      String msg = String.format("Failed parsing id field \"%s\", value \"%s\", error=\"%s\"", idProperty.getName(), id, e.toString());
      GearsBuilder.log(msg, LogLevel.WARNING);
      if(this.writeThrough) {
        List<GearsFuture<Serializable>> l = threadLocal.get();
        if(l == null) {
          l = new ArrayList<GearsFuture<Serializable>>();
          threadLocal.set(l);
        }
        GearsFuture<Serializable> f = new GearsFuture<Serializable>();
        l.add(f);
        f.setError(msg);
      } 
      throw new Exception(msg);
    }
    
    String[] command;
    Stream<String> commandStreamInit = Stream.of("XADD", String.format("%s-{%s}", streamName, GearsBuilder.hashtag()), "*", 
                                             Connector.ENTETY_NAME_STR, keySplit[0], idProperty.getName(), keySplit[1], 
                                             Connector.SOURCE_STR, name);
    
    Map<String, String> value = record.getHashVal();
    // null value here will be considered as delete
    if(value != null) {
      Stream<String> commandStream = Stream.concat(commandStreamInit, Stream.of(Connector.EVENT_STR, "hset"));
      
      // verify schema:
      Map<String, String> valuesToWrite = new HashMap<String, String>();
      for(PropertyData pd : propertyMappings.values()) {
        String val = null;
        try {
          val = value.get(pd.getName());
          if(val != null) {
            pd.convert(val);
            valuesToWrite.put(pd.getName(), val);
          }else if(!pd.isNullable()) {
            throw new Exception(String.format("mandatory \"%s\" value is not set", pd.getName()));
          }
        }catch(Exception e) {
          String msg = String.format("Failed parsing acheme for field \"%s\", value \"%s\", error=\"%s\"", pd.getName(), val, e.toString());
          GearsBuilder.log(msg, LogLevel.WARNING);
          if(this.writeThrough) {
            List<GearsFuture<Serializable>> l = threadLocal.get();
            if(l == null) {
              l = new ArrayList<GearsFuture<Serializable>>();
              threadLocal.set(l);
            }
            GearsFuture<Serializable> f = new GearsFuture<Serializable>();
            l.add(f);
            f.setError(msg);
          } 
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

    String streamId = (String)GearsBuilder.execute(command); // Write to stream
    
    if(this.writeThrough) {
      List<GearsFuture<Serializable>> l = threadLocal.get();
      if(l == null) {
        l = new ArrayList<GearsFuture<Serializable>>();
        threadLocal.set(l);
      }
      GearsFuture<Serializable> f = new GearsFuture<Serializable>();
      l.add(f);
      Tuple<String, GearsFuture<Serializable>> t = new Tuple<String, GearsFuture<Serializable>>(streamId, f);
      queue.add(t);
    } 
  }

  @Override
  public void onUnregistered() throws Exception {
    Connector c = Connector.GetConnector(connector);
    c.removeSource(this);
    sources.remove(name);
  }

  @Override
  public void onRegistered(String registrationId) throws Exception {
    this.registrationId = registrationId;
    RGHibernate.getOrCreate(this.connector).AddSource(this.name, this.xmlDef);
    sources.put(this.name, this);
    System.setProperty("javax.xml.bind.JAXBContextFactory", "org.eclipse.persistence.jaxb.JAXBContextFactory");
  }
  
  @Override
  public String toString() {
    return String.format("name: %s, connector: %s, xmlDef: %s, idName: %s", name, connector, xmlDef, idProperty.getName());
  }
  
  @Override
  public Iterator<Object> iterator() {
    List<Object> s = new ArrayList<>();
    s.add("name");
    s.add(name);
    s.add("hashPrefix");
    s.add(hashPrefix);
    s.add("connector");
    s.add(connector);
    s.add("idProperty");
    s.add(idProperty);
    s.add("Mappings");
    s.add(propertyMappings.values());
    s.add("WritePolicy");
    s.add(writeThrough ? "writeThrough" : "writeBehind");
    
    return s.iterator();
  }

  public PropertyData getPropertyMapping(String name) {
    return propertyMappings.get(name);
  }

  public PropertyData getIdProperty() {
    return idProperty;
  }
  
  public void unregister() {
    GearsBuilder.execute("RG.UNREGISTER", registrationId);
  }

  public String getStreamName() {
    return streamName;
  }

  public String getRegistrationId() {
    return registrationId;
  }

  public HashMap<String, PropertyData> getPropertyMappings() {
    return propertyMappings;
  }

  public boolean isWriteThrough() {
    return writeThrough;
  }

  public String getHashPrefix() {
    return hashPrefix;
  }

  public void setHashPrefix(String hashPrefix) {
    this.hashPrefix = hashPrefix;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setConnector(String connector) {
    this.connector = connector;
  }

  public void setXmlDef(String xmlDef) {
    this.xmlDef = xmlDef;
  }

  public void setStreamName(String streamName) {
    this.streamName = streamName;
  }

  public void setRegistrationId(String registrationId) {
    this.registrationId = registrationId;
  }

  public void setIdProperty(PropertyData idProperty) {
    this.idProperty = idProperty;
  }

  public void setPropertyMappings(HashMap<String, PropertyData> propertyMappings) {
    this.propertyMappings = propertyMappings;
  }

  public void setWriteThrough(boolean writeThrough) {
    this.writeThrough = writeThrough;
  }
}
