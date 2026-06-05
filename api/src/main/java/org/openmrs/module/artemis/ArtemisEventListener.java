/**
 * This Source Code Form is subject to the terms of the Mozilla Public License,
 * v. 2.0. If a copy of the MPL was not distributed with this file, You can
 * obtain one at http://mozilla.org/MPL/2.0/. OpenMRS is also distributed under
 * the terms of the Healthcare Disclaimer located at http://openmrs.org/license.
 *
 * Copyright (C) OpenMRS Inc. OpenMRS is a registered trademark and the OpenMRS
 * graphic logo is a trademark of OpenMRS Inc.
 */
package org.openmrs.module.artemis;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.MessageHandler;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.openmrs.event.BrokerIncomingEvent;
import org.openmrs.event.BrokerOutgoingEvent;
import org.openmrs.event.EventPayload;
import org.openmrs.event.EventPublisher;
import org.openmrs.api.context.Context;
import org.openmrs.event.outbox.OutboxEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import javax.annotation.PreDestroy;

@Component("artemis.ArtemisEventListener")
public class ArtemisEventListener {
	
	private static final Logger log = LoggerFactory.getLogger(ArtemisEventListener.class);

	private final ArtemisTask artemisTask;
	private final ObjectMapper objectMapper;
	private final EventPublisher eventPublisher;

	private ServerLocator locator;
	private ClientSessionFactory factory;
	private ClientSession session;
	private ClientProducer producer;
	private ClientConsumer consumer;

	public ArtemisEventListener(ArtemisTask artemisTask, ObjectMapper objectMapper, EventPublisher eventPublisher) {
		this.artemisTask = artemisTask;
		this.objectMapper = objectMapper;
		this.eventPublisher = eventPublisher;
	}

	private synchronized void ensureConnected() throws ArtemisException {
		try {
			if (session == null || session.isClosed()) {
				String brokerUri = artemisTask.getBrokerUri();
				if (locator == null) {
					locator = ActiveMQClient.createServerLocator(brokerUri);
				}
				if (factory == null) {
					factory = locator.createSessionFactory();
				}
				session = factory.createSession();
				// Create an anonymous producer (not bound to a specific address)
				producer = session.createProducer();

				// Setup incoming message consumer
				String incomingQueue = Context.getRuntimeProperties().getProperty("artemis.incomingQueue", "openmrs.incoming");
				try {
					// Attempt to create the queue if it doesn't already exist
					session.createQueue(incomingQueue, incomingQueue, true);
				} catch (Exception e) {
					// Queue likely already exists, safe to ignore
				}

				consumer = session.createConsumer(incomingQueue);
				consumer.setMessageHandler(new MessageHandler() {
					@Override
					public void onMessage(ClientMessage message) {
						try {
							message.acknowledge();
							String payload = message.getBodyBuffer().readString();

							BrokerIncomingEvent<String> incomingEvent = new BrokerIncomingEvent<>(incomingQueue, payload);
							eventPublisher.publishEvent(incomingEvent);

							log.debug("Received and published incoming event from Artemis: {}", payload);
						} catch (Exception e) {
							log.error("Failed to process incoming Artemis message", e);
						}
					}
				});

				// Session MUST be started to receive messages
				session.start();
				log.info("ArtemisEventListener connected to Artemis broker successfully at {}", brokerUri);
			}
		} catch (Exception e) {
			throw new ArtemisException("Failed to connect to Artemis", e);
		}
	}

	@EventListener
	public void handleEvent(BrokerOutgoingEvent<?> event) throws ArtemisException {
		ensureConnected();

		ClientMessage message;
		try {
			message = session.createMessage(event.isDurable());

			if (event.getPayload() instanceof EventPayload) {
				message.getBodyBuffer().writeString(((EventPayload) event.getPayload()).toPayload());
			} else if (event.getPayload() instanceof InputStream) {
				// Highly efficient streaming for large files without loading them into RAM
				message.setBodyInputStream((InputStream) event.getPayload());
			} else {
				message.getBodyBuffer().writeString(objectMapper.writeValueAsString(event.getPayload()));
			}
		} catch (Exception e) {
			throw new ArtemisException("Failed to create Artemis message", e);
		}

		try {
			producer.send(event.getTarget(), message);
			log.debug("Published event to Artemis: {}", event);
		} catch (Exception e) {
			// If the connection drops, clear the session so it can be re-established on the next event
			try {
				if (session != null) {
					session.close();
				}
			} catch (Exception onClose) {
				log.error("Failed to close Artemis session", onClose);
			}
			session = null;
			producer = null;
			consumer = null;
			throw new ArtemisException("Failed to publish event to Artemis. Broker might not be ready yet.", e);
		}
	}
	
	@PreDestroy
	public void cleanup() {
		try {
			if (consumer != null) consumer.close();
			if (producer != null) producer.close();
			if (session != null) session.close();
			if (factory != null) factory.close();
			if (locator != null) locator.close();
			log.info("ArtemisEventListener disconnected.");
		} catch (Exception e) {
			throw new ArtemisException("Error shutting down Artemis producer", e);
		}
	}
}