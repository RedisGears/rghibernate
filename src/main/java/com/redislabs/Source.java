package com.redislabs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
  private String name;
  private String connector;
  private String xmlDef;
  private String streamName;
  private String idName;
  private List<String> propertyMappings;
  private boolean writeThrough;
  
  private static ThreadLocal<List<GearsFuture<Serializable>>> threadLocal = new ThreadLocal<>();;
  
  private static Map<String, Source> sources = new HashMap<>();
  
  public static Source getSource(String name) {
    return sources.get(name);    
  }
  
  public static Collection<Source> getAllSources() {
    return sources.values();    
  }
  
  public Source(String name, String connector, String xmlDef, boolean writeThrough) {
    this.name = name;
    this.connector = connector;
    this.xmlDef = xmlDef;
    this.propertyMappings = new ArrayList<String>();
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
    String enteryName = pc.getEntityName();
    idName = pc.getIdentifierProperty().getName();
    Iterator<Property> iter = pc.getPropertyIterator();
    while(iter.hasNext()) {
      Property p = iter.next();
      propertyMappings.add(String.format("%s -> %s", p.getName(), ((Column)p.getColumnIterator().next()).getName()));
    }
    
    tempRegistry.close();
   
    KeysReader reader = new KeysReader().
        setPattern(enteryName + ":*").
        setEventTypes(new String[] {"hset", "hmset", "del", "changed"});
        
    GearsBuilder<KeysReaderRecord> builder = GearsBuilder.CreateGearsBuilder(reader, this.toString());
    builder.foreach(this).
    register(ExecutionMode.SYNC, this, this);
    
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
          ()->{writeThroughIsRegistered=true;}, ()->{writeThroughIsRegistered=false;});
      
      CommandOverrider delco = new CommandOverrider().setCommand("del");
      GearsBuilder.CreateGearsBuilder(delco, "del command override").
      asyncMap(asyncMapper).register(ExecutionMode.ASYNC_LOCAL, 
          ()->{writeThroughIsRegistered=true;}, ()->{writeThroughIsRegistered=false;});
      
      CommandOverrider hmsetco = new CommandOverrider().setCommand("hmset");
      GearsBuilder.CreateGearsBuilder(hmsetco, "hmsetco command override").
      asyncMap(asyncMapper).register(ExecutionMode.ASYNC_LOCAL, 
          ()->{writeThroughIsRegistered=true;}, ()->{writeThroughIsRegistered=false;});
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
    Map<String, String> value = record.getHashVal();
    String key = record.getKey();
    String[] keySplit = key.split(":");

    String[] command;
    Stream<String> commandStream = Stream.of("XADD", String.format("%s-{%s}", streamName, GearsBuilder.hashtag()), "*", "entityName", keySplit[0], idName, keySplit[1], "event", record.getEvent());
    if(record.getEvent().charAt(0) != 'd') {
      Stream<String> fieldsStream = value.entrySet().stream()
          .flatMap(entry -> Stream.of(entry.getKey(), entry.getValue()));

      command = Stream.concat(commandStream, fieldsStream).toArray(String[]::new);
    }else {
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
    // TODO Auto-generated method stub
    
  }

  @Override
  public void onRegistered() throws Exception {
    RGHibernate.getOrCreate(this.connector).AddSource(this.name, this.xmlDef);
    sources.put(this.name, this);
    System.setProperty("javax.xml.bind.JAXBContextFactory", "org.eclipse.persistence.jaxb.JAXBContextFactory");
  }
  
  @Override
  public String toString() {
    return String.format("name: %s, connector: %s, xmlDef: %s, idName: %s", name, connector, xmlDef, idName);
  }
  
  @Override
  public Iterator<Object> iterator() {
    List<Object> s = new ArrayList<>();
    s.add("name");
    s.add(name);
    s.add("connector");
    s.add(connector);
    s.add("idName");
    s.add(idName);
    s.add("Mappings");
    s.add(propertyMappings);
    s.add("WritePolicy");
    s.add(writeThrough ? "writeThrough" : "writeBehind");
    return s.iterator();
  }
  
}
