package com.redislabs;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.stream.Collectors;

import org.hibernate.Session;
import org.hibernate.Transaction;

import gears.GearsBuilder;
import gears.readers.KeysReader;
import gears.records.BaseRecord;
import gears.records.HashRecord;
import gears.records.StringRecord;

public class WriteBehind {

  public static void main() {
    System.setProperty("javax.xml.bind.JAXBContextFactory", "org.eclipse.persistence.jaxb.JAXBContextFactory");
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(WriteBehind.class.getClassLoader());

      RGHibernate rgHibernate = new RGHibernate(getConfig("hibernate.cfg.xml"));
      rgHibernate.addMapping(getConfig("Professor.hbm.xml"));
      rgHibernate.addMapping(getConfig("Student.hbm.xml"));
      Session session = rgHibernate.openSession();

      KeysReader reader = new KeysReader("*")
          .setReadValues(true)
          .setNoScan(false);

      new GearsBuilder(reader)
//      .Collect()
      .Foreach(r -> {
        HashRecord value = (HashRecord) ((HashRecord) r).Get("value");

        Map map = value.getHashMap().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));

        StringRecord key = (StringRecord) ((HashRecord) r).Get("key");
        String[] keySplit = key.toString().split(":");
        map.put("id", Integer.valueOf(keySplit[1]));

        Transaction transaction = session.beginTransaction();
        session.saveOrUpdate(keySplit[0], map);
        transaction.commit();

      }).Register();
    } finally {
      Thread.currentThread().setContextClassLoader(contextClassLoader);
    }

  }

  private static String getConfig(String file) {
    InputStream inputStream = WriteBehind.class.getClassLoader().getResourceAsStream(file);
    return new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.joining("\n"));
  }
}
