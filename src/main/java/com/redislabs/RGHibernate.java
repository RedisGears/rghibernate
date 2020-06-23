package com.redislabs;

import java.io.Closeable;
import java.io.IOException;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;

public class RGHibernate implements Closeable {

  private final MetadataSources sources;
  private volatile SessionFactory sessionFactory;

  public RGHibernate(String configuration) {
      StandardServiceRegistry registry = new StandardServiceRegistryBuilder()
          .configure( InMemoryURLFactory.getInstance().build("configuration", configuration))
          .build();
      sources = new MetadataSources(registry);
  }

  public String addMapping(String mapping) {
    
    sources.addURL(InMemoryURLFactory.getInstance().build("mapping", mapping));    
    Metadata metadata = sources.getMetadataBuilder().build();
    
    SessionFactory currentSessionFactory = sessionFactory;
    sessionFactory = metadata.getSessionFactoryBuilder().build();
    currentSessionFactory.close(); //close previous factory
    
    return metadata.getEntityBindings().iterator().next().getEntityName();
  }

  public Session openSession() {
    return sessionFactory.openSession();
  }

  @Override
  public void close() throws IOException {
    sessionFactory.close();
  }
}

