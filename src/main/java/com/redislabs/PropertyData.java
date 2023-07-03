package com.redislabs;

import org.hibernate.type.AbstractStandardBasicType;
import org.hibernate.type.TimestampType;
import org.hibernate.type.Type;

import java.io.Serializable;
import java.sql.Date;

public class PropertyData implements Serializable{

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private String name;
  private String columnName;
  private boolean isNullable;
  private Type type;
  private AbstractStandardBasicType<?> adType;
  private boolean isId;
  
  public PropertyData() {}
  
  public PropertyData(String name, String columnName, Type type, boolean isNullable, boolean isId) {
      this.name = name;
      this.type = type;
      this.columnName = columnName;
      this.isNullable = isNullable;
      this.isId = isId;
      
      if(this.type instanceof AbstractStandardBasicType<?>) {
        this.adType = (AbstractStandardBasicType<?>)this.type;
      }
  }
  
  public Object convertToObject(String val) {
    if(adType == null) {
      // best effort, keep it as string.
      return val;
    }
    if(this.type instanceof TimestampType) {
      // try first to parse as timestamp
      try {
        long timestamp = Long.parseLong(val);
        return new Date(timestamp);
      }catch(Exception e) {
        
      }
    }
    return adType.fromString(val);
  }
  
  public String convertToStr(Object val) {
    if(this.type instanceof TimestampType && (val instanceof java.sql.Timestamp)) {
      // try first to parse as timestamp
      try {
        return Long.toString(((java.sql.Timestamp)val).getTime());
      }catch(Exception e) {
        
      }
    }
    // best effort, use toString on val.
    return val.toString();
  }

  public String getName() {
    return name;
  }

  public String getColumnName() {
    return columnName;
  }

  public String getType() {
    return type.getName();
  }
  
  public boolean isNullable() {
    return isNullable;
  }
  
  public boolean isId() {
    return isId;
  }

  @Override
  public String toString() {
    return String.format("%s -> %s (type: %s, mandatory: %b, isId: %b)", getName(), getColumnName(), getType(), !isNullable(), isId());
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public void setNullable(boolean isNullable) {
    this.isNullable = isNullable;
  }

  public void setType(Type type) {
    this.type = type;
  }

  public void setId(boolean isId) {
    this.isId = isId;
  } 
 
}
