package com.redislabs;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import java.util.stream.Stream;

import org.hibernate.Session;
import org.hibernate.Transaction;

import gears.ExecutionMode;
import gears.GearsBuilder;
import gears.operations.ForeachOperation;
import gears.records.KeysReaderRecord;
import gears.readers.CommandReader;
import gears.readers.KeysReader;
import gears.readers.StreamReader;
import gears.readers.StreamReader.FailurePolicy;

public class WriteBehind implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final AtomicReference<Session> sessionRef = new AtomicReference<>();
  private static final AtomicReference<RGHibernate> hibernateRef = new AtomicReference<>();

  public static void main() {
    WriteBehind.registerOnStream();
    WriteBehind.registerOnCommands();
  }

  private static void registerOnChanges(String mapping) {

    String entity = addEntity(mapping);
    KeysReader reader = new KeysReader(entity + ":*").setEventTypes(new String[] { "hset" }).setReadValues(true)
        .setNoScan(false);

    String orgHashTag = GearsBuilder.hashtag();

    GearsBuilder.CreateGearsBuilder(reader)
    .foreach(new ForeachOperation<KeysReaderRecord>() {
		
  		private static final long serialVersionUID = 1L;
  		
  		private transient String streamName = String.format("_Stream-{%s}", GearsBuilder.hashtag());
  
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
    }).register(ExecutionMode.SYNC, () -> {
      // All shards but the original shard should set the mapping
      if (!GearsBuilder.hashtag().contentEquals(orgHashTag)) {
        addEntity(mapping);
      }
      // Init the session here so it will happen on each shard
      Session orgSession = WriteBehind.sessionRef.getAndSet(WriteBehind.hibernateRef.get().openSession());
      if (orgSession != null) {
        orgSession.close();
      }
    }, () -> {});
  }

  private static void registerOnStream() {
	StreamReader streamReader = new StreamReader()
			.setPattern(String.format("_Stream-*"))
			.setBatchSize(100)
			.setDuration(1000)
			.setFailurePolicy(FailurePolicy.RETRY)
			.setFailureRertyInterval(5000);

    GearsBuilder.CreateGearsBuilder(streamReader).foreach(r -> {

      Map<String, byte[]> value = (Map<String, byte[]>) r.get("value");

      Map<String, String> map = value.entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey, e -> new String(e.getValue())));

      Session session = WriteBehind.sessionRef.get();
      Transaction transaction = session.beginTransaction();
      session.saveOrUpdate(map.remove("entityName"), map);
      transaction.commit();
      session.clear();

    }).register(ExecutionMode.ASYNC_LOCAL, () -> {
    }, () -> {
      Session session = sessionRef.getAndSet(null);
      if (session != null) {
        session.close();
      }
      RGHibernate hibernate = hibernateRef.getAndSet(null);
      if (hibernate != null) {
        hibernate.close();
      }
    }

    );
  }

  private static String addEntity(String mapping) {
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(WriteBehind.class.getClassLoader());
      RGHibernate rgHibernate = WriteBehind.hibernateRef.get();
      return rgHibernate.addMapping(mapping);

    } finally {
      Thread.currentThread().setContextClassLoader(contextClassLoader);
    }
  }

  private static void registerOnCommands() {
    // Register on set schema
    CommandReader readerSchema = new CommandReader().setTrigger("set_schema");

    GearsBuilder.CreateGearsBuilder(readerSchema).map(args -> {
      byte[] value = (byte[]) args[1];
      registerOnChanges(new String(value));
      return "OK";
    }).register(ExecutionMode.SYNC);

    // Register on set connection
    CommandReader readerConnection = new CommandReader().setTrigger("set_connection");
    GearsBuilder.CreateGearsBuilder(readerConnection).map(args -> {
      System.setProperty("javax.xml.bind.JAXBContextFactory", "org.eclipse.persistence.jaxb.JAXBContextFactory");
      ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
      try {
        Thread.currentThread().setContextClassLoader(WriteBehind.class.getClassLoader());

        byte[] value = (byte[]) args[1];
        WriteBehind.hibernateRef.set(new RGHibernate(new String(value)));
        return "OK";
      } finally {
        Thread.currentThread().setContextClassLoader(contextClassLoader);
      }
    }).collect().register(ExecutionMode.ASYNC);
  }

}
