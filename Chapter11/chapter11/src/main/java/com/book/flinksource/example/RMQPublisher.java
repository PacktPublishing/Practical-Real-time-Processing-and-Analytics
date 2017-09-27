package com.book.flinksource.example;

import java.io.IOException;
import java.util.Scanner;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RMQPublisher {
	public static void main(String[] args) throws IOException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUsername("guest");
		factory.setPassword("guest");
		factory.setVirtualHost("/");
		factory.setHost("localhost");
		factory.setPort(5672);
		Connection newConnection = factory.newConnection();

		Channel channel = newConnection.createChannel();

		Scanner scanner = new Scanner(System.in);
		String message = "";
		while(!message.equals("exit")){
			System.out.println("Enter your message");
			message = scanner.next();
			channel.queueDeclare("flink-test", true, false, false, null);
			channel.basicPublish("", "flink-test", new BasicProperties.Builder()
					.correlationId(java.util.UUID.randomUUID().toString()).build(), message.getBytes());
		}
		
		scanner.close();
		channel.close();
		newConnection.close();
	}
}
