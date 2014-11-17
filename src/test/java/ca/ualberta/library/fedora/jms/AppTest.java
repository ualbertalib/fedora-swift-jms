package ca.ualberta.library.fedora.jms;

import static org.junit.Assert.*;
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.AfterClass;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.ConnectionFactory;
import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.Destination;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.MessageConsumer;
import javax.jms.ObjectMessage;
import javax.naming.Context;

import org.apache.activemq.*;

public class AppTest {

	private static Connection connection;
	private static Session session;
	private static Destination destination;
	private static Destination replyToDestination;
	
    private class TextListener implements MessageListener {
 
        /**
         * Casts the message to a TextMessage and displays its text.
         * A non-text message is interpreted as the end of the message 
         * stream, and the message listener sets its monitor state to all 
         * done processing messages.
         *
         * @param message  the incoming message
         */
        public void onMessage(Message message) {
            if (message instanceof TextMessage) {
                TextMessage  msg = (TextMessage) message;

                try {
                    System.out.println("CONSUMER THREAD: Reading message: " 
                                       + msg.getText());
                } catch (JMSException e) {
                    System.out.println("Exception in onMessage(): " 
                                       + e.toString());
                }
            }
        }
    }
    
	@BeforeClass
	public static void setup() throws JMSException {
		ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
		connection = connectionFactory.createConnection();
		connection.start();
		session = connection.createSession(true, Session.SESSION_TRANSACTED);
		destination = session.createTopic("Test.Topic");
		
		
	}
	
	@AfterClass
	public static void cleanup() throws JMSException {
		session.close();
		connection.stop();
		connection.close();
	}
	
	@Test
	public void testA() throws JMSException {
		MessageConsumer consumer = session.createConsumer(destination);
		TextListener listener = new TextListener();
		consumer.setMessageListener(listener);
		
		MessageProducer producer = session.createProducer(destination);
		Message message = session.createTextMessage("Test");
		producer.send(message);
		session.commit();
	}
	
}
