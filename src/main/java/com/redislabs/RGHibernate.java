package com.redislabs;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;


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
        .configure(InMemoryURLFactory.getInstance().build("configuration", args[0])).build();
    MetadataSources sources = new MetadataSources(registry);
    for (int i = 1; i < args.length; ++i) {
      sources.addURL(InMemoryURLFactory.getInstance().build("mapping", args[i]));
    }
    Metadata metadata = sources.getMetadataBuilder().build();

    factory = metadata.getSessionFactoryBuilder().build();
    session = factory.openSession();
  }

  public Session getSession() {
    return session;
  }

  @SuppressWarnings("PMD.EmptyCatchBlock")
  public void recreateSession() {
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

    generateSession();
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
    session.close();
    factory.close();

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
