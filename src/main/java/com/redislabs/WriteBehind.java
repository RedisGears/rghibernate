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
  
  public static final int VERSION = 102;
  
  public static class UpdateInfo{
    
    private Collection<Connector> connectors;
    private Collection<Source> sources;
    
    public UpdateInfo() {}
    
    public UpdateInfo(Collection<Connector> connectors, Collection<Source> sources) {
      this.connectors = connectors;
      this.sources = sources;
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
    String res = objectMapper.writeValueAsString(new UpdateInfo(Connector.getAllConnectors(), Source.getAllSources()));
    objectMapper.getTypeFactory().clearCache();
    TypeFactory.defaultInstance().clearCache();
    return res;
  }
  
  public static void main(String[] args) throws Exception {
    
    if(args.length == 1 && args[0].equals("version")) {
      int patch = VERSION % 100;
      int minor = (VERSION / 100) % 100;
      int major = (VERSION / 10000);
      System.out.print(String.format("%d.%d.%d\r\n", major, minor, patch));
      return;
    }
    
    UpdateInfo updateInfo = null;
    Object[] sessions = (Object[])((Object[])GearsBuilder.execute("RG.JDUMPSESSIONS"))[1];
    List<Object[]> oldVersions = Arrays.stream(sessions).map(Object[].class::cast).
        filter(s->s[3].equals("com.redislabs.WriteBehind") && !s[9].toString().equals("0")).collect(Collectors.toList());
    if(oldVersions.size() > 2) {
      throw new Exception("Found more then one WriteBehind versions installed, fatal!!");
    }
    if(oldVersions.size() == 2) {
      Object[] oldVersion = oldVersions.stream().filter(r-> (Long)r[5] < VERSION).findFirst().orElse(null);
      if(oldVersion == null) {
        throw new Exception("A newer version already exists");
      }
      
      GearsBuilder.log(String.format("upgrading from version %s", oldVersion[5].toString()));
      
      String sessionId = (String)(oldVersion[1]);
      String updateData = GearsBuilder.getSessionUpgradeData(sessionId);
      GearsBuilder.log(String.format("Got update data from session %s", sessionId));
      ObjectMapper objectMapper = new ObjectMapper();
      objectMapper.registerSubtypes(ReadSource.class, WriteSource.class);
      updateInfo = objectMapper.readValue(updateData, UpdateInfo.class);
      objectMapper.getTypeFactory().clearCache();
      TypeFactory.defaultInstance().clearCache();
      
      GearsBuilder.log("Unregister old registrations");
      
      if(updateInfo != null) {
        for(Source s: updateInfo.getSources()) {
          s.unregister();
        }
        for(Connector c: updateInfo.getConnectors()) {
          c.unregister();
        }
      }
      
      GearsBuilder.log("Unregister managemen operations");
      Object[] registrations = (Object[])GearsBuilder.execute("RG.DUMPREGISTRATIONS");
      Arrays.stream(registrations).map(Object[].class::cast).filter(r->r[9].toString().contains(String.format("'SessionId':'%s'", sessionId)))
      .forEach(r->GearsBuilder.execute("RG.UNREGISTER", r[1].toString()));
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
