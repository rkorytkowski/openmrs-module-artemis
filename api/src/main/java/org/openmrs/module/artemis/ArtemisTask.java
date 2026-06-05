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

import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.remoting.server.RemotingService;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.activemq.artemis.spi.core.remoting.Acceptor;
import org.openmrs.GlobalProperty;
import org.openmrs.api.AdministrationService;
import org.openmrs.api.context.Context;
import org.openmrs.event.EventPublisher;
import org.openmrs.scheduler.TaskContext;
import org.openmrs.scheduler.TaskHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.net.InetAddress;

@Component("artemis.ArtemisTask")
public class ArtemisTask implements TaskHandler<ArtemisTaskData> {
	
	private static final Logger log = LoggerFactory.getLogger(ArtemisTask.class);

	public final String ARTEMIS_URI_GP = "artemis.uri";
	
	@Autowired(required = false)
	private EventPublisher eventPublisher;

	@Autowired(required = false)
	private AdministrationService adminService;
	
	private EmbeddedActiveMQ embeddedActiveMQ;

	public ArtemisTask() {
	}

	public String getBrokerUri() {
		if (embeddedActiveMQ != null) {
			return "vm://0"; //Use in-vm
		} else {
			return Context.getRuntimeProperties().getProperty("artemis.uri", adminService.getGlobalProperty(ARTEMIS_URI_GP));
		}
	}

	@Override
	public void execute(ArtemisTaskData artemisTaskData, TaskContext taskContext) throws Exception {
		if (embeddedActiveMQ == null) {
			log.info("Starting embedded Artemis broker...");

			ConfigurationImpl config = new ConfigurationImpl()
					.addAcceptorConfiguration("in-vm", "vm://0")
					.addAcceptorConfiguration("tcp", "tcp://0.0.0.0:0") // assign a random free port
					.setSecurityEnabled(false)
					.setJMXManagementEnabled(true);

			config.parsePrefixedProperties(Context.getRuntimeProperties(), "artemis.");
			
			embeddedActiveMQ = new EmbeddedActiveMQ();
			embeddedActiveMQ.setConfiguration(config);
			embeddedActiveMQ.start();
			
			int actualPort = 61616;
			try {
				RemotingService remotingService = embeddedActiveMQ.getActiveMQServer().getRemotingService();
				Acceptor tcpAcceptor = remotingService.getAcceptor("tcp");
				if (tcpAcceptor != null) {
					actualPort = tcpAcceptor.getActualPort();
				}
			} catch (Exception e) {
				log.warn("Could not determine actual Artemis port, falling back to 61616", e);
			}

			String hostAddress = "localhost";
			try {
				hostAddress = InetAddress.getLocalHost().getHostAddress();
			} catch (Exception e) {
				log.warn("Could not determine local IP address, falling back to localhost", e);
			}

			adminService.saveGlobalProperty(new GlobalProperty(ARTEMIS_URI_GP, "tcp://" + hostAddress + ":" + actualPort));

			log.info("Embedded Artemis broker started successfully. Starting to monitor it...");

			// Loop and hold execution as long as the underlying server is reporting active
			while (embeddedActiveMQ.getActiveMQServer().isActive()) {
				// Thread sleep prevents high CPU utilization from the loop
				Thread.sleep(1000);
			}
		}
	}

	@PreDestroy
	public void stop() throws Exception {
		if (embeddedActiveMQ != null) {
			log.info("Stopping embedded Artemis broker...");
			adminService.saveGlobalProperty(new GlobalProperty(ARTEMIS_URI_GP, ""));
			embeddedActiveMQ.stop();
			log.info("Embedded Artemis broker stopped.");
		}
	}
}
