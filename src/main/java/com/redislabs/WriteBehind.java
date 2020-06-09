package com.redislabs;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import java.util.stream.Stream;

import org.hibernate.Session;
import org.hibernate.Transaction;

import gears.GearsBuilder;
import gears.records.KeysReaderRecord;
import gears.readers.CommandReader;
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
//    wb.registerOnChanges();
    wb.registerOnStream();
    wb.registerOnCommands();
  }

  private void registerOnChanges(String prefix) {
    KeysReader reader = new KeysReader(prefix + "*").setEventTypes(new String[] { "hset" }).setReadValues(true).setNoScan(false);

    new GearsBuilder(reader).foreach(r -> {
      KeysReaderRecord record = (KeysReaderRecord)r; 
      Map<String, String> value = record.getHashVal();
      String key = record.getKey();
      String[] keySplit = key.split(":");
      
      Stream<String> commandStream = Stream.of("XADD", "stream", "*", "entityName", keySplit[0],  "id", keySplit[1]);
      Stream<String> fieldsStream = value.entrySet().stream()
          .flatMap(entry -> Stream.of(entry.getKey(), entry.getValue().toString())); 
      
      String[] command = Stream.concat(commandStream, fieldsStream).toArray(String[]::new);
      
      GearsBuilder.execute(command); // Write to stream 
      
    }).register();
  }

  private void registerOnStream() {
    StreamReader streamReader = new StreamReader();

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
    CommandReader readerSchema = new CommandReader().setTrigger("set_schema");
    new GearsBuilder(readerSchema).foreach(r -> {
      

      Object[] args = (Object[])r; 
      StringRecord value = (StringRecord)args[1];
      
      RGHibernate rgHibernate = hibernateRef.get();
      String entity = rgHibernate.addMapping(value.toString());
      registerOnChanges(entity);
      
      Session orgSession = sessionRef.getAndSet(rgHibernate.openSession());
      if(orgSession!=null) {
        orgSession.close();
      }
    }).register();
    
    
    // Register on set connection 
    CommandReader readerConnection = new CommandReader().setTrigger("set_connection");
    new GearsBuilder(readerConnection).foreach(r -> {
      System.setProperty("javax.xml.bind.JAXBContextFactory", "org.eclipse.persistence.jaxb.JAXBContextFactory");
      ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
      try {
        Thread.currentThread().setContextClassLoader(WriteBehind.class.getClassLoader());

        Object[] args = (Object[])r; 
        StringRecord value = (StringRecord)args[1];
        hibernateRef.set( new RGHibernate(value.toString()));

      } finally {
        Thread.currentThread().setContextClassLoader(contextClassLoader);
      }
    }).register();
  }
  
}
