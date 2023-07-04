package com.redislabs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import gears.LogLevel;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.Property;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import gears.GearsBuilder;
import gears.operations.OnRegisteredOperation;
import gears.operations.OnUnregisteredOperation;

@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include=JsonTypeInfo.As.PROPERTY, property="type")
public abstract class Source implements OnRegisteredOperation, OnUnregisteredOperation, Iterable<Object>{
  
  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  protected static final Map<String, Source> sources = new ConcurrentHashMap<>();
  
  public static Source getSource(String name) {
    return sources.get(name);    
  }
  
  public static Collection<Source> getAllSources() {
    return sources.values();    
  }
  
  private String connector;
  private String name;
  private String xmlDef;
  private List<String> registrationIds;
  private String hashPrefix;
  
  @JsonIgnore
  private PropertyData idProperty;
  
  @JsonIgnore
  private HashMap<String, PropertyData> propertyMappings;
  
  public Source() {}
  
  public Source(String connector, String name, String xmlDef) {
    this.connector = connector;
    this.name = name;
    this.xmlDef = xmlDef;
    this.propertyMappings = new HashMap<>();
    
    String generatedXmlConf = RGHibernate.get(connector).getXmlConf();
    
    try {
      Object[] res = (Object[])  GearsBuilder.execute("RG.TRIGGER", "rghibernateGetPassword", connector);
      String dbpass = (String) res[0];
      generatedXmlConf = generatedXmlConf.replaceAll("(connection\\.password\">)[^<]*", "$1" + dbpass);
    } catch (Exception e) {
      GearsBuilder.log(String.format("Failing generating password using 'RG.TRIGGER rghibernateGetPassword' using regular xml configuration, %s.", e), LogLevel.VERBOSE);
    } finally {
      Thread.currentThread().setContextClassLoader(Source.class.getClassLoader());
    }

    StandardServiceRegistry tempRegistry = new StandardServiceRegistryBuilder()
         .configure( InMemoryURLFactory.getInstance().build("configuration", generatedXmlConf))
        .build();

    MetadataSources tempSources = new MetadataSources(tempRegistry);
    tempSources.addURL(InMemoryURLFactory.getInstance().build("mapping", xmlDef));
    Metadata metadata = tempSources.getMetadataBuilder().build();
    PersistentClass pc = metadata.getEntityBindings().iterator().next();
    hashPrefix = pc.getEntityName();
    Property idProp = pc.getIdentifierProperty();
    idProperty =  new PropertyData(idProp.getName(), 
        ((Column)idProp.getColumnIterator().next()).getName(), 
        idProp.getType(), idProp.isOptional(), true);
    Iterator<Property> iter = pc.getPropertyIterator();
    while(iter.hasNext()) {
      Property p = iter.next();
      PropertyData pd = new PropertyData(p.getName(), 
          ((Column)p.getColumnIterator().next()).getName(), 
          p.getType(), p.isOptional(), false);
      propertyMappings.put(pd.getName(), pd);
    }
    
    tempRegistry.close();
    
    sources.put(name, this);
  }
  
  @Override
  public void onUnregistered() throws Exception {
    Connector c = Connector.getConnector(connector);
    c.removeSource(this);
    sources.remove(name);
  }

  @Override
  public void onRegistered(String registrationId) throws Exception {
    if(this.registrationIds == null) {
      this.registrationIds = new ArrayList<>();
    }
    this.registrationIds.add(registrationId);
    RGHibernate.getOrCreate(this.connector).addSource(this.name, this.xmlDef);
    sources.put(this.name, this);
    System.setProperty("javax.xml.bind.JAXBContextFactory", "org.eclipse.persistence.jaxb.JAXBContextFactory");
  }
  
  public void unregister() {
    for(String id : this.registrationIds) {
      GearsBuilder.execute("RG.UNREGISTER", id);
    }
  }
  
  protected Connector getConnectorObj() {
    return Connector.getConnector(connector);
  }

  public String getConnector() {
    return connector;
  }

  public void setConnector(String connector) {
    this.connector = connector;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getXmlDef() {
    return xmlDef;
  }

  public void setXmlDef(String xmlDef) {
    this.xmlDef = xmlDef;
  }

  public List<String> getRegistrationIds() {
    return registrationIds;
  }

  public void setRegistrationIds(List<String> registrationIds) {
    this.registrationIds = registrationIds;
  }

  public String getHashPrefix() {
    return hashPrefix;
  }

  public void setHashPrefix(String hashPrefix) {
    this.hashPrefix = hashPrefix;
  }

  public PropertyData getIdProperty() {
    return idProperty;
  }

  public void setIdProperty(PropertyData idProperty) {
    this.idProperty = idProperty;
  }

  public HashMap<String, PropertyData> getPropertyMappings() {
    return propertyMappings;
  }
  
  public PropertyData getPropertyMapping(String prop) {
    return propertyMappings.get(prop);
  }

  public void setPropertyMappings(HashMap<String, PropertyData> propertyMappings) {
    this.propertyMappings = propertyMappings;
  }
  
  @Override
  public Iterator<Object> iterator() {
    return Arrays.asList("name", getName(), 
        "registrationIds", registrationIds,
        "hashPrefix", hashPrefix, 
        "connector", getConnector(),
        "idProperty", idProperty, 
        "mappings", propertyMappings.values()).iterator();
  }
}
