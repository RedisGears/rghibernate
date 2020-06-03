package com.redislabs;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import java.util.stream.Stream;

import org.hibernate.Session;
import org.hibernate.Transaction;

import gears.GearsBuilder;
import gears.readers.ExecutionMode;
import gears.readers.KeysReader;
import gears.readers.StreamReader;
import gears.records.HashRecord;
import gears.records.StringRecord;

public class WriteBehind implements Serializable{

  private static final long serialVersionUID = 1L;
  
  private final transient AtomicReference<Session> sessionRef       = new AtomicReference<>();
  private final transient AtomicReference<RGHibernate> hibernateRef = new AtomicReference<>();

  public static void main() {
    WriteBehind wb = new WriteBehind();
    wb.registerOnChanges();
    wb.registerOnStream();
    wb.registerOnCommands();
  }

  private void registerOnChanges() {
    KeysReader reader = new KeysReader("*").setEventTypes(new String[] { "hset" }).setReadValues(true).setNoScan(false);

    new GearsBuilder(reader).foreach(r -> {
      HashRecord record = (HashRecord)r; 
      HashRecord value = (HashRecord)record.get("value");
      StringRecord key = (StringRecord)record.get("key");
      String[] keySplit = key.toString().split(":");
      
      Stream<String> commandStream = Stream.of("XADD", "stream", "*", "entityName", keySplit[0],  "id", keySplit[1]);
      Stream<String> fieldsStream = value.getHashMap().entrySet().stream()
          .flatMap(entry -> Stream.of(entry.getKey(), entry.getValue().toString())); 
      
      String[] command = Stream.concat(commandStream, fieldsStream).toArray(String[]::new);
      
      GearsBuilder.execute(command); // Write to stream 
      
    }).register();
  }

  private void registerOnStream() {
    StreamReader streamReader = new StreamReader(ExecutionMode.ASYNC_LOCAL, ()->{});

    new GearsBuilder(streamReader).foreach(r -> {
      HashRecord value = (HashRecord) ((HashRecord) r).get("value");

      Map<String, String> map = value.getHashMap().entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));

      Session session = sessionRef.get();
      Transaction transaction = session.beginTransaction();
      session.saveOrUpdate(map.remove("entityName"), map);
      transaction.commit();
      session.clear();

    }).register();
  }
  
  private void registerOnCommands() {
    // Register on set schema
    KeysReader readerSchema = new KeysReader("__schema").setEventTypes(new String[] { "set" }).setReadValues(true).setNoScan(false);
    new GearsBuilder(readerSchema).foreach(r -> {
      HashRecord record = (HashRecord)r; 
      StringRecord value = (StringRecord)record.get("value");
      
      RGHibernate rgHibernate = hibernateRef.get();
      rgHibernate.addMapping(value.toString());
      Session orgSession = sessionRef.getAndSet(rgHibernate.openSession());
      if(orgSession!=null) {
        orgSession.close();
      }
    }).register();
    
    
    // Register on set connection 
    KeysReader readerConnection = new KeysReader("__connection").setEventTypes(new String[] { "set" }).setReadValues(true).setNoScan(false);
    new GearsBuilder(readerConnection).foreach(r -> {
      System.setProperty("javax.xml.bind.JAXBContextFactory", "org.eclipse.persistence.jaxb.JAXBContextFactory");
      ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
      try {
        Thread.currentThread().setContextClassLoader(WriteBehind.class.getClassLoader());

        HashRecord record = (HashRecord)r; 
        StringRecord value = (StringRecord)record.get("value");
        
        hibernateRef.set( new RGHibernate(value.toString()));

      } finally {
        Thread.currentThread().setContextClassLoader(contextClassLoader);
      }
    }).register();
  }
  
}
