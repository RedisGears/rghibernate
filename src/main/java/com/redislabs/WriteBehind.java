package com.redislabs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

import gears.ExecutionMode;
import gears.GearsBuilder;
import gears.operations.FlatMapOperation;
import gears.readers.CommandReader;

public class WriteBehind{
  
  public static final int VERSION = 0x00000114;
  public static final String DESCRIPTION = "A write behind/read through recipe for RedisGears JVM leverage hibernate for external db conectivity.";
  
  public static class UpdateInfo{
    
	private int version;
    private Collection<Connector> connectors;
    private Collection<Source> sources;
    
    public UpdateInfo() {}
    
    public UpdateInfo(int version, Collection<Connector> connectors, Collection<Source> sources) {
      this.version = version;
      this.connectors = connectors;
      this.sources = sources;
    }

    public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public Collection<Connector> getConnectors() {
      if(connectors == null) {
        return new ArrayList<>();
      }
      return connectors;
    }

    public void setConnectors(Collection<Connector> connectors) {
      this.connectors = connectors;
    }

    public Collection<Source> getSources() {
      if(sources == null) {
        return new ArrayList<>();
      }
      return sources;
    }

    public void setSources(Collection<Source> sources) {
      this.sources = sources;
    }
  }
  
  public static String getUpgradeData() throws JsonProcessingException {
    ObjectMapper objectMapper = new ObjectMapper();
    String res = objectMapper.writeValueAsString(new UpdateInfo(VERSION, Connector.getAllConnectors(), Source.getAllSources()));
    objectMapper.getTypeFactory().clearCache();
    TypeFactory.defaultInstance().clearCache();
    return res;
  }
  
  public static String getStringVersion(int version) {
    int patch = version & 0x000000FF;
    int minor = ((version & 0x0000FF00) >> 8);
    int major = ((version & 0x00FF0000) >> 16);
    return String.format("%d.%d.%d", major, minor, patch);
  }
  
  public static void main(String[] args) throws Exception {
    String verStr = getStringVersion(VERSION);
    if(args.length == 1 && args[0].equals("version")) {
      System.out.print(String.format("RGHibernate %s\r\n", verStr));
      return;
    }
    GearsBuilder.log(String.format("RGHibernate %s", verStr));
    
    String updateData = GearsBuilder.getUpgradeData();
    UpdateInfo updateInfo = null;
    if (updateData != null) {
    	ObjectMapper objectMapper = new ObjectMapper();
    	objectMapper.registerSubtypes(ReadSource.class, WriteSource.class);
    	updateInfo = objectMapper.readValue(updateData, UpdateInfo.class);
    	objectMapper.getTypeFactory().clearCache();
    	TypeFactory.defaultInstance().clearCache();
    }
    
    if (updateInfo != null) {
    	GearsBuilder.log(String.format("Upgrade from version %s", getStringVersion(updateInfo.getVersion())));
    }
    
    // add connector registration
    CommandReader newConnectorReader = new CommandReader().setTrigger("SYNC.REGISTERCONNECTOR");
    GearsBuilder.CreateGearsBuilder(newConnectorReader, "Register a new connector").
    map(r->{
      String connectorName = new String((byte[])r[1]);
      String connectorXml = new String((byte[])r[5]);
      int batchSize = Integer.parseInt(new String((byte[])r[2]));
      int duration = Integer.parseInt(new String((byte[])r[3]));
      int retryInterval = Integer.parseInt(new String((byte[])r[4]));
      if(Connector.getConnector(connectorName)!=null) {
        throw new Exception("connector already exists");
      }
      new Connector(connectorName, connectorXml, batchSize, duration, retryInterval);
      return "OK";
    }).register(ExecutionMode.SYNC);
    
    // add source registration
    CommandReader newSourceReader = new CommandReader().setTrigger("SYNC.REGISTERSOURCE");
    GearsBuilder.CreateGearsBuilder(newSourceReader, "Registe a new source").
    map(r->{
      String sourceName = new String((byte[])r[1]);
      String connectorName = new String((byte[])r[2]);
      String writePolicy = new String((byte[])r[3]);
      int timeout = 0;
      String sourceXml = null;
      boolean isWrite = true;
      boolean writeThrough = false;
      if(writePolicy.equals("WriteThrough")) {
        isWrite = true;
        writeThrough = true;
        try {
          timeout = Integer.parseInt(new String((byte[])r[4]));
        }catch (Exception e) {
          throw new Exception("Could not parse timeout argument");
        }
        sourceXml = new String((byte[])r[5]);
      }else if(writePolicy.equals("WriteBehind")) {
        isWrite = true;
        writeThrough = false;
        sourceXml = new String((byte[])r[4]);
      }else if(writePolicy.equals("ReadThrough")) {
        isWrite = false;
        try {
          timeout = Integer.parseInt(new String((byte[])r[4]));
        }catch (Exception e) {
          throw new Exception("Could not parse expire argument");
        }
        sourceXml = new String((byte[])r[5]);
      } else {
        throw new Exception("Write policy should be either WriteThrough/WriteBehind/ReadThrough");
      }
      
      if(WriteSource.getSource(sourceName) != null) {
        throw new Exception("source already exists");
      }
      Connector c = Connector.getConnector(connectorName);
      if(c == null) {
        throw new Exception("connector does not exists");
      }
      Source s = null;
      if (isWrite) {
        s = new WriteSource(sourceName, connectorName, sourceXml, writeThrough, timeout);
      }else {
        s = new ReadSource(sourceName, connectorName, sourceXml, timeout);
      }
      c.addSource(s);
      return "OK";
    }).register(ExecutionMode.SYNC);
    
    // remove source
    CommandReader newRemoveSourceReader = new CommandReader().setTrigger("SYNC.UNREGISTERSOURCE");
    GearsBuilder.CreateGearsBuilder(newRemoveSourceReader, "Unregiste source").
    map(r->{
      String sourceName = new String((byte[])r[1]);
      Source s = WriteSource.getSource(sourceName);
      if(s == null) {
        throw new Exception("source does exists");
      }
      s.unregister();
      return "OK";
    }).register(ExecutionMode.SYNC);
    
    // remove connector
    CommandReader newRemoveConnectorReader = new CommandReader().setTrigger("SYNC.UNREGISTERCONNECTOR");
    GearsBuilder.CreateGearsBuilder(newRemoveConnectorReader, "Unregiste connector").
    map(r->{
      String connectorName = new String((byte[])r[1]);
      Connector c = Connector.getConnector(connectorName);
      if(c == null) {
        throw new Exception("connector does exists");
      }
      c.unregister();
      return "OK";
    }).register(ExecutionMode.SYNC);
    
    // general information
    CommandReader syncInfoReader = new CommandReader().setTrigger("SYNC.INFO");
    GearsBuilder.CreateGearsBuilder(syncInfoReader, "General info about sync").
    flatMap(new FlatMapOperation<Object[], Serializable>() {

      /**
       * 
       */
      private static final long serialVersionUID = 1L;

      @Override
      public Iterable<Serializable> flatmap(Object[] r) throws Exception {
        String subInfoCommand = null;
        
        if(r.length > 1) {
          subInfoCommand = new String((byte[])r[1]);
        }
        
        if("CONNECTORS".equals(subInfoCommand)) {
          return Connector.getAllConnectors().stream().map(Serializable.class::cast).collect(Collectors.toList());
        }
        
        if("SOURCES".equals(subInfoCommand)) {
          return WriteSource.getAllSources().stream().map(Serializable.class::cast).collect(Collectors.toList());
        }
        
        if("GENERAL".equals(subInfoCommand)) {
          LinkedList<Serializable> res = new LinkedList<>();
          res.push("NConnector");
          res.push(Integer.toString(Connector.getAllConnectors().size()));
          res.push("NSources");
          res.push(Integer.toString(WriteSource.getAllSources().size()));
          
          return res;
        }
        
        throw new Exception("no such option");
      }
      
    }).register(ExecutionMode.SYNC);
    
    if(updateInfo != null) {
      GearsBuilder.log("Upgrade registrations");
      
      for(Connector c: updateInfo.getConnectors()) {
        new Connector(c.getName(), c.getXmlDef(), c.getBatchSize(), c.getDuration(), c.getRetryInterval());
      }
      
      for(Source temp: updateInfo.getSources()) {
        if(temp instanceof WriteSource) {
          WriteSource s = (WriteSource)temp;
          new WriteSource(s.getName(), s.getConnector(), s.getXmlDef(), s.isWriteThrough(), s.getTimeout());
        } else if(temp instanceof ReadSource) {
          ReadSource s = (ReadSource)temp;
          new ReadSource(s.getName(), s.getConnector(), s.getXmlDef(), s.getExpire());
        }
      }
    }
  }
}
