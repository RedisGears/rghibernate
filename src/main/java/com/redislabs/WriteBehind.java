package com.redislabs;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
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

public class WriteBehind {

  private static final AtomicReference<Session> sessionRef = new AtomicReference<>();

  public static void main() throws Exception {
    registerOnChanges();
//    registerOnStream();

  }

  private static void registerOnChanges() {
    KeysReader reader = new KeysReader("*").setEventTypes(new String[] { "hset" }).setReadValues(true).setNoScan(false);

    new GearsBuilder(reader).foreach(r -> {
      HashRecord value = (HashRecord) ((HashRecord) r).get("value");

      Stream<String> commandStream = Stream.of("XADD", "stream", "*");
      String[] command = Stream.concat(commandStream, value.getHashMap().entrySet().stream()
          .flatMap(entry -> Stream.of(entry.getKey(), entry.getValue().toString()))).toArray(String[]::new);
      
      GearsBuilder.execute(command);
    }).register();
  }

  private static void registerOnStream() {
    StreamReader streamReader = new StreamReader(ExecutionMode.ASYNC_LOCAL, () -> {
      System.setProperty("javax.xml.bind.JAXBContextFactory", "org.eclipse.persistence.jaxb.JAXBContextFactory");
      ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
      try {
        Thread.currentThread().setContextClassLoader(WriteBehind.class.getClassLoader());

        RGHibernate rgHibernate = new RGHibernate(getConfig("hibernate.cfg.xml"));
        rgHibernate.addMapping(getConfig("Professor.hbm.xml"));
        rgHibernate.addMapping(getConfig("Student.hbm.xml"));
        sessionRef.set(rgHibernate.openSession());
      } finally {
        Thread.currentThread().setContextClassLoader(contextClassLoader);
      }
    });

    new GearsBuilder(streamReader).foreach(r -> {
      HashRecord value = (HashRecord) ((HashRecord) r).get("value");

      Map map = value.getHashMap().entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));

      StringRecord key = (StringRecord) ((HashRecord) r).get("key");
      String[] keySplit = key.toString().split(":");
      map.put("id", Integer.valueOf(keySplit[1]));

      Session session = sessionRef.get();
      Transaction transaction = session.beginTransaction();
      session.saveOrUpdate(keySplit[0], map);
      transaction.commit();
      session.clear();

    }).register();
  }

  private static String getConfig(String file) {
    InputStream inputStream = WriteBehind.class.getClassLoader().getResourceAsStream(file);
    return new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.joining("\n"));
  }
}
