package com.redislabs;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;

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

  public RGHibernate(String[] args) {
    this.args = args;
  }
  
  public void generateSession() {
    StandardServiceRegistry registry = new StandardServiceRegistryBuilder()
        .configure( InMemoryURLFactory.getInstance().build("configuration", args[0]))
        .build();
    MetadataSources sources = new MetadataSources(registry);
    for(int i = 1 ; i < args.length ; ++i) {
      sources.addURL(InMemoryURLFactory.getInstance().build("mapping", args[i]));
    }
    Metadata metadata = sources.getMetadataBuilder().build();
    
    session = metadata.getSessionFactoryBuilder().build().openSession(); 
  }

  public Session getSession() {
    return session;
  }

  @Override
  public void close() throws IOException {
    session.close();
  }
}

