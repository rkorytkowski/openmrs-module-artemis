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

import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.openmrs.api.context.Context;
import org.openmrs.event.EventPublisher;
import org.openmrs.scheduler.TaskContext;
import org.openmrs.scheduler.TaskHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;

@Component("artemis.ArtemisTask")
public class ArtemisTask implements TaskHandler<ArtemisTaskData> {
	
	private static final Logger log = LoggerFactory.getLogger(ArtemisTask.class);
	
	@Autowired(required = false)
	private EventPublisher eventPublisher;
	
	private EmbeddedActiveMQ embeddedActiveMQ;

	public ArtemisTask() {
	}

	@Override
	public void execute(ArtemisTaskData artemisTaskData, TaskContext taskContext) throws Exception {
		if (embeddedActiveMQ == null) {
			log.info("Starting embedded Artemis broker...");

			ConfigurationImpl config = new ConfigurationImpl()
					.addAcceptorConfiguration("in-vm", "vm://0")
					.addAcceptorConfiguration("tcp", "tcp://0.0.0.0:61616")
					.setSecurityEnabled(false);

			config.parsePrefixedProperties(Context.getRuntimeProperties(), "artemis.");
			
			embeddedActiveMQ = new EmbeddedActiveMQ();
			embeddedActiveMQ.setConfiguration(config);
			embeddedActiveMQ.start();
			
			log.info("Embedded Artemis broker started successfully.");
		}
	}

	@PreDestroy
	public void stop() throws Exception {
		if (embeddedActiveMQ != null) {
			log.info("Stopping embedded Artemis broker...");
			embeddedActiveMQ.stop();
			log.info("Embedded Artemis broker stopped.");
		}
	}
}
