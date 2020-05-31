package com.redislabs;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.hibernate.Session;
import org.hibernate.Transaction;
import org.junit.Test;

public class RGHibernateTest {


  @Test
  public void test() {

    RGHibernate rgHibernate = new RGHibernate(getConfig("hibernate.cfg.xml"));

    // Add new Student mapping
    rgHibernate.addMapping(getConfig("Student.hbm.xml"));
    try (Session session = rgHibernate.openSession()) {

      Transaction transaction = session.beginTransaction();
      Map item1 = new HashMap();
      item1.put( "id", 33);
      item1.put( "firstName", "Ron" );
      item1.put( "lastName", "Don" );
      item1.put( "email", "ron.don@tau.ac.il" );
      session.saveOrUpdate("Student", item1);
      transaction.commit();
      
      Transaction transaction2 = session.beginTransaction();
      Map item2 = new HashMap();
      item2.put( "id", 12);
      item2.put( "firstName", "Danni" );
      item2.put( "lastName", "Din" );
      item2.put( "email", "danni.din@tau.ac.il" );
      session.saveOrUpdate("Student", item2);
      transaction2.commit();
    } 
    
    // Add new Professor mapping
    rgHibernate.addMapping(getConfig("Professor.hbm.xml"));
    try (Session session = rgHibernate.openSession()) {

      Transaction transaction3 = session.beginTransaction();
      Map item3 = new HashMap();
      item3.put( "id", 51);
      item3.put( "firstName", "John");
      item3.put( "lastName", "Scott");
      item3.put( "email", "john.scott@tau.ac.il");
      session.saveOrUpdate("Professor", item3);
      transaction3.commit();
      
      Transaction transaction4 = session.beginTransaction();
      Map item4 = new HashMap();
      item4.put( "id", 12);
      item4.put( "firstName", "Danni");
      item4.put( "lastName", "Chin");
      item4.put( "email", "danni.chin@tau.ac.il");
      session.saveOrUpdate("Student", item4);
      transaction4.commit();
    } 

  }

  private String getConfig(String file) {
    InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(file);
    return new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.joining("\n"));
  }

}