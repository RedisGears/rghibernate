package com.redislabs;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;

import gears.GearsBuilder;


public class RGHibernate implements Closeable, Serializable {

  private static final long serialVersionUID = 1L;

  private static Map<String, RGHibernate> hibernateConnections = new HashMap<String, RGHibernate>();
  
  public static RGHibernate getOrCreate(String name) {
    RGHibernate ret = hibernateConnections.get(name);
    if(ret == null) {
      ret = new RGHibernate(name);
    }
    return ret;
  }
  
  public static RGHibernate get(String name) {
    return hibernateConnections.get(name);
  }
  
  private String name;
  private String xmlConf;
  private Map<String, String> sources;
  private transient Session session = null;
  private transient SessionFactory factory = null;
  private transient StandardServiceRegistry registry = null;

  public RGHibernate(String name) {
    this.name = name;
    this.sources = new HashMap<>();
    hibernateConnections.put(this.name, this);
  }
  
  public String getXmlConf() {
    return xmlConf;
  }

  public void setXmlConf(String xmlConf) {
    this.xmlConf = xmlConf;
  }
  
  public void CloseSession() {
    if(session == null) {
      return;
    }
    
    try {
      session.clear();
    } catch (Exception e) {
      // TODO: handle exception
    }

    try {
      session.close();
    } catch (Exception e) {
      // TODO: handle exception
    }

    try {
      factory.close();
    } catch (Exception e) {
      // TODO: handle exception
    }

    try {
      registry.close();
    } catch (Exception e) {
      // TODO: handle exception
    }
    
    session = null;
  }

  private void GenerateSession() {
    registry = new StandardServiceRegistryBuilder()
        .configure(InMemoryURLFactory.getInstance().build("configuration", xmlConf)).build();
    MetadataSources sources = new MetadataSources(registry);
    Collection<String> srcs = this.sources.values();
    for (String src : srcs) {
      sources.addURL(InMemoryURLFactory.getInstance().build("mapping", src));
    }
    Metadata metadata = sources.getMetadataBuilder().build();

    factory = metadata.getSessionFactoryBuilder().build();
    session = factory.openSession();
    GearsBuilder.log(String.format("%s connector Connected successfully", name));
  }
  
  public void AddSource(String sourceName, String sourceXmlDef) {
    sources.put(sourceName, sourceXmlDef);
    synchronized (this) {
      CloseSession();
    }
  }

  public Session getSession() {
    if(session == null) {
      GenerateSession();
    }
    return session;
  }


  protected void cancelTimers() {
    try {
      for (Thread thread : Thread.getAllStackTraces().keySet())
        if (thread.getClass().getSimpleName().equals("TimerThread"))
          cancelTimer(thread);
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }

  private void cancelTimer(Thread thread) throws Exception {
    // Timer::cancel
    
    Field f = thread.getClass().getDeclaredField("queue");
    f.setAccessible(true);
    Object queue = f.get(thread);
    synchronized (queue) {
      f = thread.getClass().getDeclaredField("newTasksMayBeScheduled");
      f.setAccessible(true);
      f.set(thread, false);
      Method m = queue.getClass().getDeclaredMethod("clear");
      m.setAccessible(true);
      m.invoke(queue);
      queue.notify();
    }
  }

  @Override
  public void close() throws IOException {
    CloseSession();

    try {
      Enumeration<Driver> de = DriverManager.getDrivers();
      while (de.hasMoreElements()) {
        Driver d = de.nextElement();
        if (d.getClass().getClassLoader() == RGHibernate.class.getClassLoader()) {
          DriverManager.deregisterDriver(d);
        }

      }
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    // Closing timer thread for oracle driver not to leak..
    cancelTimers();
  }
}
