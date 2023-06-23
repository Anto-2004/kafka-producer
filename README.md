# Kafka-Producer

Kafka-Producer is a library that simplifies connecting and sending custom messages and objects to a server or application running kafka

## Usage
We establish a connection with the server

```java
ProducerKafka producer = new ProducerKafka("localhost",9092);
```
Once the ProducerKafka object has been instantiated we have the send method to send simple text strings or complex objects
```java
producer.send("Hello kafka!");
//or
producer.send(obj, Obj.class);

//example
Book book = new Book();
book.setTitle("Book example");

//now we send the obj book via kafka
producer.send(book, Book.class);
```
We can clear the channel
```java
producer.flush();
```
We can close the connection with the server
```java
producer.close();
```
