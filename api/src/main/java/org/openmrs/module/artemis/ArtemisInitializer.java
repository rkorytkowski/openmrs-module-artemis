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

import org.openmrs.api.context.Context;
import org.openmrs.scheduler.SchedulerService;
import org.openmrs.util.PrivilegeConstants;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component("artemis.ArtermisInitializer")
public class ArtemisInitializer implements SmartInitializingSingleton {
	
	private final SchedulerService schedulerService;
	
	public static final String EMBEDDED_ARTEMIS_TASK = "Embedded Artemis";
	
	public static final String EMBEDDED_ARTEMIS_TASK_UUID = "f5d4f293-a2aa-4bf9-825c-5c78f4223ba2";
	
	public ArtemisInitializer(SchedulerService schedulerService) {
		this.schedulerService = schedulerService;
	}
	
	@Override
	public void afterSingletonsInstantiated() {
		try {
			if (!Context.isSessionOpen()) {
				Context.openSession();
			}
			Context.addProxyPrivilege(PrivilegeConstants.MANAGE_SCHEDULER);
			// Scheduled recurrently, but it should be running indefinitely if no errors occur.
			// If an error occurs, the task is retried 3 times within a few seconds and if it is still failing, it gets
			// re-scheduled after 30 seconds.
			schedulerService.scheduleRecurrently(EMBEDDED_ARTEMIS_TASK_UUID, new ArtemisTaskData(),
			    Duration.ofSeconds(30), EMBEDDED_ARTEMIS_TASK);
		}
		finally {
			Context.removeProxyPrivilege(PrivilegeConstants.MANAGE_SCHEDULER);
		}
	}
}
