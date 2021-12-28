package com.example.kafka.streams.demoKafkaStreams.model;

import java.util.Date;

public class Product {

  private String id;

  private String name;

  private long start;

  private long end;

  public Product(String id, String name, long start, long end) {
    this.id = id;
    this.name = name;
    this.start = start;
    this.end = end;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public long getStart() {
    return start;
  }

  public void setStart(long start) {
    this.start = start;
  }

  public long getEnd() {
    return end;
  }

  public void setEnd(long end) {
    this.end = end;
  }
}
