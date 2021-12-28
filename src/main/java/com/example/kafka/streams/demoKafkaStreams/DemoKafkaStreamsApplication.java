package com.example.kafka.streams.demoKafkaStreams;

import java.time.Duration;
import java.util.Arrays;
import java.util.Random;
import java.util.function.Function;
import java.util.function.Supplier;

import com.example.kafka.streams.demoKafkaStreams.model.Product;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class DemoKafkaStreamsApplication {

  public static void main(String[] args) {
    SpringApplication.run(DemoKafkaStreamsApplication.class, args);
  }

  @Bean
  public Supplier<String> nameSupplier() {
    return () -> {
      System.out.println("Generating new name...");
      return "Product-" + new Random().nextInt(1000);
    };
  }

  @Bean
  public Function<KStream<Object, String>, KStream<String, Product>> kafkaStreamCount() {
    return input -> input
        .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
        .map((key, value) -> new KeyValue<>(value, value))
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .windowedBy(TimeWindows.of(Duration.ofSeconds(5)))
        .count(Materialized.as("word-counts-state-store"))
        .toStream()
        .map((key, value) -> new KeyValue<>(key.key(), new Product(key.key(), value.toString(), key.window().start(), key.window().end())));
  }

}
