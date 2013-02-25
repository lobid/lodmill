/* Copyright 2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.metatext.launching;

import java.io.File;

import org.culturegraph.metaflow.Metaflow;
import org.eclipse.core.resources.IResource;
import org.eclipse.core.resources.ResourcesPlugin;
import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.ILog;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.core.runtime.Platform;
import org.eclipse.core.runtime.Status;
import org.eclipse.debug.core.ILaunch;
import org.eclipse.debug.core.ILaunchConfiguration;
import org.eclipse.debug.core.model.ILaunchConfigurationDelegate;

public class MetatextLaunchConfigurationDelegate implements
		ILaunchConfigurationDelegate {

	private static final String BUNDLE = "org.lobid.metatext";
	private static final ILog LOG = Platform.getLog(Platform.getBundle(BUNDLE));
	public static final String FILE_NAME = "filename";

	public void launch(ILaunchConfiguration configuration, String mode,
			ILaunch launch, IProgressMonitor monitor) throws CoreException {
		System.out.println("Launching... config attributes: "
				+ configuration.getAttributes());
		monitor.beginTask("Metatext Workflow", 10);
		final String file = configuration.getAttribute(FILE_NAME, "");
		IResource member = ResourcesPlugin.getWorkspace().getRoot()
				.findMember(file);
		monitor.worked(3);
		monitor.subTask("Running Workflow");
		runWorkflow(monitor, member);
		monitor.subTask("Refreshing Workspace");
		member.getParent().refreshLocal(IResource.DEPTH_INFINITE, monitor);
		monitor.done();
	}

	private void runWorkflow(IProgressMonitor monitor, IResource member) {
		if (!monitor.isCanceled()) {
			String flow = new File(member.getLocationURI()).getAbsolutePath();
			LOG.log(new Status(Status.INFO, BUNDLE, "Running file: " + flow));
			try {
				Metaflow.main(new String[] { "-f", flow });
			} catch (Exception e) {
				e.printStackTrace();
				LOG.log(new Status(Status.ERROR, BUNDLE, e.getMessage(), e));
			}
		}
		monitor.worked(7);
	}
}
