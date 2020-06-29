package com.redislabs;

import java.io.Serializable;
import java.sql.DriverManager;
import java.util.ArrayList;
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
import org.mariadb.jdbc.Driver;

import com.fasterxml.jackson.databind.ObjectMapper;

import gears.ExecutionMode;
import gears.GearsBuilder;
import gears.LogLevel;
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
    int majorV = 99;
    int minorV = 99;
    int patchV = 99;
    
    public MetaData() {
      
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
  
  private static void unregisterOldVersions(MetaData metaData) throws Exception {
    GearsBuilder.log("Unregister old versions");
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
  }
  
  public static void main(String[] args) throws Exception {
    System.setProperty("javax.xml.bind.JAXBContextFactory", "org.eclipse.persistence.jaxb.JAXBContextFactory");
    Thread.currentThread().setContextClassLoader(WriteBehind.class.getClassLoader());
    
    MetaData metaData = new MetaData();
    
    if(args.length < 2) {
      throw new Exception("Not enough arguments given");
    }
    
    ObjectMapper objectMapper = new ObjectMapper();
    String registrationsDesc = objectMapper.writeValueAsString(metaData);
    
    // we need this to make sure we are not leaking
    objectMapper.getTypeFactory().clearCache();
    
    
    unregisterOldVersions(metaData);
    
    GearsBuilder.log(String.format("Register %s", registrationsDesc));
    
    String connectionXml = args[0];
    
    StandardServiceRegistry tempRegistry = new StandardServiceRegistryBuilder()
        .configure( InMemoryURLFactory.getInstance().build("configuration", connectionXml))
        .build();
    
    String uuid = UUID.randomUUID().toString();
    
    // Created Keys Readers
    
    for(int i = 1 ; i < args.length ; ++i) {
      MetadataSources tempSources = new MetadataSources(tempRegistry);
      tempSources.addURL(InMemoryURLFactory.getInstance().build("mapping", args[i]));
      Metadata metadata = tempSources.getMetadataBuilder().build(); 
      String enteryName = metadata.getEntityBindings().iterator().next().getEntityName();
      
     
      KeysReader reader = new KeysReader().
          setPattern(enteryName + ":*").
          setEventTypes(new String[] {"hset", "hmset"});
          
      GearsBuilder.CreateGearsBuilder(reader, registrationsDesc).
      foreach(new ForeachOperation<KeysReaderRecord>() {
        
        private static final long serialVersionUID = 1L;

        private transient String streamName = String.format("_Stream-%s-{%s}", uuid, GearsBuilder.hashtag());
        
        protected Object readResolve() {
          this.streamName = String.format("_Stream-%s-{%s}", uuid, GearsBuilder.hashtag());
          return this;
        }
        
        @Override
        public void foreach(KeysReaderRecord record) throws Exception {
          Map<String, String> value = record.getHashVal();
          String key = record.getKey();
          String[] keySplit = key.split(":");
    
          Stream<String> commandStream = Stream.of("XADD", streamName, "*", "entityName", keySplit[0], "id", keySplit[1]);
          Stream<String> fieldsStream = value.entrySet().stream()
              .flatMap(entry -> Stream.of(entry.getKey(), entry.getValue()));
    
          String[] command = Stream.concat(commandStream, fieldsStream).toArray(String[]::new);
    
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
        .setFailureRertyInterval(5000);

    GearsBuilder.CreateGearsBuilder(streamReader, registrationsDesc).foreach(r -> {

        Map<String, byte[]> value = (Map<String, byte[]>) r.get("value");

        Map<String, String> map = value.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> new String(e.getValue())));

        GearsBuilder.log(rghibernate.toString());
        Session session = rghibernate.getSession();
        Transaction transaction = session.beginTransaction();
        session.saveOrUpdate(map.remove("entityName"), map);
        transaction.commit();
        session.clear();

    }).register(ExecutionMode.ASYNC_LOCAL, () -> {
      System.setProperty("javax.xml.bind.JAXBContextFactory", "org.eclipse.persistence.jaxb.JAXBContextFactory");
      Thread.currentThread().setContextClassLoader(WriteBehind.class.getClassLoader());
      rghibernate.generateSession();
    }, () -> {
      rghibernate.close();
    });
  }
}
