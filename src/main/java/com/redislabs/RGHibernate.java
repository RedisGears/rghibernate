package com.redislabs;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Iterator;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.SessionFactoryBuilder;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;

import gears.GearsBuilder;

public class RGHibernate implements Closeable, Serializable {

  private static final long serialVersionUID = 1L;

  private String[] args;
  private transient Session session = null;
  private transient SessionFactory factory = null;
  private transient StandardServiceRegistry registry = null;
  
  public RGHibernate(String[] args) {
    this.args = args;
  }
  
  public void generateSession() {
    registry = new StandardServiceRegistryBuilder()
        .configure( InMemoryURLFactory.getInstance().build("configuration", args[0]))
        .build();
    MetadataSources sources = new MetadataSources(registry);
    for(int i = 1 ; i < args.length ; ++i) {
      sources.addURL(InMemoryURLFactory.getInstance().build("mapping", args[i]));
    }
    Metadata metadata = sources.getMetadataBuilder().build();
    
    factory = metadata.getSessionFactoryBuilder().build();
    session = factory.openSession(); 
  }

  public Session getSession() {
    return session;
  }

  @Override
  public void close() throws IOException {
    session.close();
    factory.close();
    
    try {
      Enumeration<Driver> de = DriverManager.getDrivers();
      while(de.hasMoreElements()) {
        Driver d = de.nextElement();
        if(d.getClass().getClassLoader() == RGHibernate.class.getClassLoader()) {
          DriverManager.deregisterDriver(d);
        }
        
      }
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}

