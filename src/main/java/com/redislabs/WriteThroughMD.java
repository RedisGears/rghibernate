package com.redislabs;

import gears.GearsFuture;

public class WriteThroughMD {
  
  private String streamID;
  private GearsFuture<String> f;
  private long startTime;
  private long timeout;
  
  public WriteThroughMD(String streamID, GearsFuture<String> f, long timeout) {
    this.streamID = streamID;
    this.f = f;
    this.timeout = timeout;
    this.startTime = System.currentTimeMillis();
  }
  
  public boolean TryFree(String currStreamID) throws Exception {
    if(currStreamID != null && currStreamID.compareTo(streamID) >= 0) {
      f.setResult(streamID);
      return true;
    }
    
    long currTime = System.currentTimeMillis();
    if (startTime + timeout < currTime) {
      f.setResult("-ERR Write Timed out");
      return true;
    }
    
    return false;
  }
  
  
}
