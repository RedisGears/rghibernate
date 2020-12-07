package com.redislabs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.boot.registry.internal.StandardServiceRegistryImpl;
import org.hibernate.service.Service;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.tuple.IdentifierProperty;

import gears.ExecutionMode;
import gears.GearsBuilder;
import gears.GearsFuture;
import gears.LogLevel;
import gears.operations.AccumulateOperation;
import gears.operations.ForeachOperation;
import gears.operations.MapOperation;
import gears.operations.OnRegisteredOperation;
import gears.operations.OnUnregisteredOperation;
import gears.readers.StreamReader;
import gears.readers.StreamReader.FailurePolicy;
import oracle.ucp.common.waitfreepool.Tuple;

public class Connector implements ForeachOperation<ArrayList<HashMap<String,Object>>>,
AccumulateOperation<HashMap<String,Object>, ArrayList<HashMap<String, Object>>>,
OnRegisteredOperation,
OnUnregisteredOperation, Iterable<String>,
MapOperation<HashMap<String, Object>, HashMap<String, Object>>{
  
  public static final String ENTETY_NAME_STR = "__entityName__";
  public static final String EVENT_STR = "__event__";
  public static final String SOURCE_STR = "__source__";
  
  class MyStandardServiceInitiator implements StandardServiceInitiator<Service>{

    private Map values;
    
    public String getUrl() {
      return (String) values.get("hibernate.connection.url");
    }
    
    public String getUser() {
      return (String) values.get("hibernate.connection.username");
    }
    
    public String getDriverClass() {
      return (String) values.get("hibernate.connection.driver_class");
    }
    
    public String getDialect() {
      return (String) values.get("hibernate.dialect");
    }
    
    @Override
    public Class<Service> getServiceInitiated() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Service initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
      // TODO Auto-generated method stub
      values = configurationValues;
      return null;
    }
    
  }
  
  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public static Map<String, Connector> connectors = new ConcurrentHashMap<>();
  
  static Collection<Connector> getAllConnectors() {
    return connectors.values();    
  }
  
  static Connector GetConnector(String name) {
    return connectors.get(name);
  }
  
  private String name;
  private String uuid;
  private String xmlDef;
  private String url;
  private String driverClass;
  private String userName;
  private String dialect;
  private String registrationId;
  private transient RGHibernate connector;
  
  public Connector() {}
  
  public Connector(String name, String xmlDef, int batchSize, int duration, int retryInterval) {
    this.name = name;
    this.xmlDef = xmlDef;
    
    System.setProperty("javax.xml.bind.JAXBContextFactory", "org.eclipse.persistence.jaxb.JAXBContextFactory");
    Thread.currentThread().setContextClassLoader(WriteBehind.class.getClassLoader());
    StandardServiceRegistryImpl tempRegistry = (StandardServiceRegistryImpl)new StandardServiceRegistryBuilder()
        .configure( InMemoryURLFactory.getInstance().build("configuration", this.xmlDef))
        .build();
    
    MyStandardServiceInitiator initiator = this.new MyStandardServiceInitiator();
    tempRegistry.initiateService(initiator);
    
    this.url = initiator.getUrl();
    this.driverClass = initiator.getDriverClass();
    this.userName = initiator.getUser();
    this.dialect = initiator.getDialect();
    
    tempRegistry.close();
    
    uuid = UUID.randomUUID().toString();
    String streamName = String.format("_Stream-%s-%s-*", name, uuid);
    StreamReader streamReader = new StreamReader()
        .setPattern(streamName)
        .setBatchSize(batchSize)
        .setDuration(duration)
        .setFailurePolicy(FailurePolicy.RETRY)
        .setFailureRertyInterval(retryInterval);

    GearsBuilder<HashMap<String,Object>> builder = GearsBuilder.CreateGearsBuilder(streamReader, String.format("%s connector", name));
    
    builder.map(this).accumulate(this).foreach(this).
    map(ArrayList<HashMap<String, Object>>::size).
    register(ExecutionMode.ASYNC_LOCAL, this, this);
  }
  
  public String getUuid() {
    return uuid;
  }

  @Override
  public void onUnregistered() throws Exception {
    connector.close();
    connectors.remove(this.name);
  }
  
  public void addSource(Source s) {
    connector.AddSource(s.getName(), s.getXmlDef());
  }
  
  public void removeSource(Source s) {
    connector.RemoveSource(s.getName());
  }

  @Override
  public void onRegistered(String registrationId) throws Exception {
    this.registrationId = registrationId;
    connector = RGHibernate.getOrCreate(this.name);
    connector.setXmlConf(this.xmlDef);
    connectors.put(this.name, this);
    System.setProperty("javax.xml.bind.JAXBContextFactory", "org.eclipse.persistence.jaxb.JAXBContextFactory");
    Thread.currentThread().setContextClassLoader(WriteBehind.class.getClassLoader());
  }

  @Override
  public ArrayList<HashMap<String, Object>> accumulate(ArrayList<HashMap<String, Object>> a,
      HashMap<String, Object> r) throws Exception {
    // TODO Auto-generated method stub
    if(a == null) {
      a = new ArrayList<>();
    }
    a.add(r);
    return a;
  }
  
  @Override
  public HashMap<String, Object> map(HashMap<String, Object> r) throws Exception {
    Map<String, byte[]> value = (Map<String, byte[]>) r.get("value");
    Map<String, String> map = value.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> new String(e.getValue())));
    
    String sourceName = map.get(SOURCE_STR);
    
    Source source = Source.getSource(sourceName);
    
    Map<String, Object> newMap = new HashMap<String, Object>();
    PropertyData idProperty =  source.getIdProperty();
    
    String val = map.remove(idProperty.getName());
    Object convertedVal = null;
    try {
      convertedVal = idProperty.convert(val);
    }catch (Exception e) {
      String msg = String.format("Can not conver id property %s val %s, error='%s'", idProperty.getName(), val, e.toString());
      GearsBuilder.log(msg, LogLevel.WARNING);
      throw new Exception(msg);
    }

    newMap.put(idProperty.getName(), convertedVal);
    newMap.put(ENTETY_NAME_STR, map.remove(ENTETY_NAME_STR));
    newMap.put(EVENT_STR, map.remove(EVENT_STR));
    newMap.put(SOURCE_STR, map.remove(SOURCE_STR));
    
    for(String key : map.keySet()) {
      val = map.get(key);
      
      PropertyData pm = null;
      convertedVal = null;
      try {
        pm = source.getPropertyMapping(key);
        convertedVal = pm.convert(val);
      }catch (Exception e) {
        String msg = String.format("Can not find property mapping for %s val %s, error='%s'", key, val, e.toString());
        GearsBuilder.log(msg, LogLevel.WARNING);
        throw new Exception(msg);
      }
      
      
      newMap.put(key, convertedVal);
    }
    
    r.put("value", newMap);
    
    return r;
  }

  @Override
  public void foreach(ArrayList<HashMap<String, Object>> record) throws Exception {
    String lastStreamId = null;
    synchronized (this.connector) {
      try {
        Session session = connector.getSession();
        Transaction transaction = session.beginTransaction();
        boolean isMerge = true;
        
        for(Map<String, Object> r: record) {
          lastStreamId = new String((byte[])r.get("id"));
          Map<String, Object> map = (Map<String, Object>) r.get("value");
          String sourceName = (String)map.remove(SOURCE_STR);
          
          String event = (String)map.remove(EVENT_STR);
          if(event.charAt(0) != 'd') {
            if(!isMerge) {
              transaction.commit();
              session.clear();
              transaction = session.beginTransaction();
              isMerge = true;
            }
            session.merge((String)map.remove(ENTETY_NAME_STR), map);
          }else {
            if(isMerge) {
              transaction.commit();
              session.clear();
              transaction = session.beginTransaction();
              isMerge = false;
            }
            Source source = Source.getSource(sourceName);
            Object o = session.get((String)map.remove(ENTETY_NAME_STR), (Serializable)map.get(source.getIdProperty().getName()));
            // o can be null on hdel that removed the last field
            if(o != null) {
              session.delete(o);
            }
          }
        }
        
        transaction.commit();
        session.clear();
      }catch (Exception e) {
        String msg = String.format("Failed commiting transaction error='%s'", e.toString());
        GearsBuilder.log(msg, LogLevel.WARNING);
        connector.CloseSession();
        throw new Exception(msg);
      }
      
      if(lastStreamId != null) {
        while(!Source.queue.isEmpty()) {
          Tuple<String, GearsFuture<Serializable>> t = Source.queue.peek();
          String s = t.get1();
          GearsFuture<Serializable> f = t.get2();
          
          if(lastStreamId.compareTo(s) >= 0) {
            f.setResult(s);
            Source.queue.remove();
            continue;
          }
          break;
        }
      }
    }
    
  }
  
  @Override
  public String toString() {
    return String.format("name: %s, xmlDef: %s", this.name, this.xmlDef);
  }

  @Override
  public Iterator<String> iterator() {
    List<String> s = new ArrayList<>();
    s.add("name");
    s.add(name);
    s.add("url");
    s.add(url);
    s.add("driverClass");
    s.add(driverClass);
    s.add("userName");
    s.add(userName);
    s.add("dialect");
    s.add(dialect);
    return s.iterator();
  }
  
  public void unregister() throws Exception {
    if(connector.NumSources() > 0) {
      throw new Exception("Can't unregister connector with sources");
    }
    GearsBuilder.execute("RG.UNREGISTER", registrationId);
  }

}
