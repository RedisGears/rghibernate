package com.redislabs;

import java.io.Serializable;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import java.util.stream.Stream;

import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.mapping.PersistentClass;
import org.mariadb.jdbc.Driver;

import com.fasterxml.jackson.databind.ObjectMapper;

import gears.ExecutionMode;
import gears.GearsBuilder;
import gears.LogLevel;
import gears.operations.AccumulateOperation;
import gears.operations.ForeachOperation;
import gears.records.KeysReaderRecord;
import gears.readers.KeysReader;
import gears.readers.StreamReader;
import gears.readers.StreamReader.FailurePolicy;

public class WriteBehind implements Serializable {

  private static final long serialVersionUID = 1L;
  
  static class MetaData implements Comparable<MetaData>, Serializable{
    private static final long serialVersionUID = 1L;
    
    String name = "JWriteBehind";
    String desc = "Write behind java implementation";
    String uuid = null;
    int majorV = 99;
    int minorV = 99;
    int patchV = 99;
    
    public MetaData() {
      
    }
    
    public String getUuid() {
      return uuid;
    }

    public void setUuid(String uuid) {
      this.uuid = uuid;
    }
    
    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getDesc() {
      return desc;
    }

    public void setDesc(String desc) {
      this.desc = desc;
    }

    public int getMajorV() {
      return majorV;
    }

    public void setMajorV(int majorV) {
      this.majorV = majorV;
    }

    public int getMinorV() {
      return minorV;
    }

    public void setMinorV(int minorV) {
      this.minorV = minorV;
    }

    public int getPatchV() {
      return patchV;
    }

    public void setPatchV(int patchV) {
      this.patchV = patchV;
    }
    
    @Override
    public int compareTo(MetaData arg0) {
      if(this.majorV == 99 && this.minorV == 99 && this.patchV == 99) {
        return 1;
      }
      
      if(this.majorV > this.majorV) {
        return 1;
      }
      if(this.majorV < this.majorV) {
        return -1;
      }
      
      if(this.minorV > this.minorV) {
        return 1;
      }
      if(this.minorV < this.minorV) {
        return -1;
      }
      
      if(this.patchV > this.patchV) {
        return 1;
      }
      if(this.patchV < this.patchV) {
        return -1;
      }
      
      return 0;
      
    }
    
    public String toString() {
      return String.format("%s-%d.%d.%d", name, majorV, minorV, patchV);
    }
  }
  
  private static String unregisterOldVersions(MetaData metaData) throws Exception {
    GearsBuilder.log("Unregister old versions");
    String uuid = null;
    List<String> idsToUnregister = new ArrayList<>();
    Object[] registrations = (Object[])GearsBuilder.execute("RG.DUMPREGISTRATIONS");
    for(Object o : registrations) {
      Object[] registration = (Object[])o;
      String id = (String)registration[1];
      String desc = (String)registration[5];
      ObjectMapper objectMapper = new ObjectMapper();
      MetaData md = null;
      try {
        md = objectMapper.readValue(desc, MetaData.class);
      }catch (Exception e) {
        GearsBuilder.log(e.toString(), LogLevel.WARNING);
        continue;
      }
      
      // we need this to make sure we are not leaking
      objectMapper.getTypeFactory().clearCache();
      
      if(metaData.compareTo(md) <= 0) {
        String msg = String.format("Found newer write behind version, curr_version='%s', found_versoin='%s'", metaData, md);
        GearsBuilder.log(msg, LogLevel.WARNING);
        throw new Exception(msg);
      }
      uuid = md.getUuid();
      idsToUnregister.add(id);
    }
    
    for(String id : idsToUnregister) {
      GearsBuilder.log(String.format("Unregister %s",  id), LogLevel.WARNING);
      String res = (String)GearsBuilder.execute("RG.UNREGISTER", id);
      if(!res.equals("OK")) {
        String msg = String.format("Failed unregister old registration, registration='%s', command_response='%s'", id, res);
        GearsBuilder.log(msg, LogLevel.WARNING);
        throw new Exception(msg);
      }
    }
    
    GearsBuilder.log("Done unregistered old versions");
    
    if(uuid == null) {
      uuid = UUID.randomUUID().toString();
    }
    
    return uuid;
  }
  
  public static void main(String[] args) throws Exception {
    System.setProperty("javax.xml.bind.JAXBContextFactory", "org.eclipse.persistence.jaxb.JAXBContextFactory");
    Thread.currentThread().setContextClassLoader(WriteBehind.class.getClassLoader());
    
    if(args.length < 2) {
      throw new Exception("Not enough arguments given");
    }
    
    MetaData metaData = new MetaData();
    
    String uuid = unregisterOldVersions(metaData);

    metaData.setUuid(uuid);
    
    ObjectMapper objectMapper = new ObjectMapper();
    String registrationsDesc = objectMapper.writeValueAsString(metaData);
    // we need this to make sure we are not leaking
    objectMapper.getTypeFactory().clearCache();
    
    GearsBuilder.log(String.format("Register %s", registrationsDesc));
    
    String connectionXml = args[0];
    
    StandardServiceRegistry tempRegistry = new StandardServiceRegistryBuilder()
        .configure( InMemoryURLFactory.getInstance().build("configuration", connectionXml))
        .build();
    
    // Created Keys Readers
    
    for(int i = 1 ; i < args.length ; ++i) {
      MetadataSources tempSources = new MetadataSources(tempRegistry);
      tempSources.addURL(InMemoryURLFactory.getInstance().build("mapping", args[i]));
      Metadata metadata = tempSources.getMetadataBuilder().build();
      PersistentClass pc = metadata.getEntityBindings().iterator().next();
      String enteryName = pc.getEntityName();
      String idName = pc.getIdentifierProperty().getName();
      
     
      KeysReader reader = new KeysReader().
          setPattern(enteryName + ":*").
          setEventTypes(new String[] {"hset", "hmset", "del"});
          
      GearsBuilder.CreateGearsBuilder(reader, registrationsDesc).
      foreach(new ForeachOperation<KeysReaderRecord>() {
        
        private static final long serialVersionUID = 1L;

        private transient String streamName = String.format("_Stream-%s", uuid);
        
        protected Object readResolve() {
          this.streamName = String.format("_Stream-%s", uuid);
          return this;
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
    
          GearsBuilder.execute(command); // Write to stream    
        }
        
      }).register(ExecutionMode.SYNC, ()->{
        System.setProperty("javax.xml.bind.JAXBContextFactory", "org.eclipse.persistence.jaxb.JAXBContextFactory");
      }, ()->{});
    }
    
    tempRegistry.close();
    
    
    // Created Stream Reader
    
    RGHibernate rghibernate = new RGHibernate(args);
    
    StreamReader streamReader = new StreamReader()
        .setPattern(String.format("_Stream-%s-*", uuid))
        .setBatchSize(100)
        .setDuration(1000)
        .setFailurePolicy(FailurePolicy.RETRY)
        .setFailureRertyInterval(5);

    GearsBuilder.CreateGearsBuilder(streamReader, registrationsDesc).
    accumulate(new AccumulateOperation<HashMap<String,Object>, ArrayList<HashMap<String, Object>>>() {

      
      private static final long serialVersionUID = 1L;

      @Override
      public ArrayList<HashMap<String, Object>> accumulate(ArrayList<HashMap<String, Object>> a,
          HashMap<String, Object> r) throws Exception {
        
        if(a == null) {
          a = new ArrayList<HashMap<String, Object>>();
        }
        a.add(r);
        return a;
      }
      
    }).
    foreach(records -> {
      
      try {
        Session session = rghibernate.getSession();
        Transaction transaction = session.beginTransaction();
        boolean isMerge = true;
        
        for(Map<String, Object> r: records) {
          Map<String, byte[]> value = (Map<String, byte[]>) r.get("value");
  
          Map<String, String> map = value.entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey, e -> new String(e.getValue())));
          
          String event = map.remove("event");
          if(event.charAt(0) != 'd') {
            if(!isMerge) {
              transaction.commit();
              session.clear();
              transaction = session.beginTransaction();
              isMerge = true;
            }
            session.merge(map.remove("entityName"), map);
          }else {
            if(isMerge) {
              transaction.commit();
              session.clear();
              transaction = session.beginTransaction();
              isMerge = false;
            }
            session.delete(map.remove("entityName"), map);
          }
        }
        
        transaction.commit();
        session.clear();
      }catch (Exception e) {
        rghibernate.recreateSession();
        throw e;
      }
    }).
    map(r->r.size()).
    register(ExecutionMode.ASYNC_LOCAL, () -> {
      System.setProperty("javax.xml.bind.JAXBContextFactory", "org.eclipse.persistence.jaxb.JAXBContextFactory");
      Thread.currentThread().setContextClassLoader(WriteBehind.class.getClassLoader());
      rghibernate.generateSession();
    }, () -> {
      rghibernate.close();
    });
  }
}
