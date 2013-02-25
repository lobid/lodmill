/* Copyright 2013 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package org.lobid.metatext.launching;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

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
			File flowFile = new File(member.getLocationURI());
			LOG.log(new Status(Status.INFO, BUNDLE, "Running file: " + flowFile));
			try {
				String flowWithAbsolutePaths = resolveDotInPaths(
						flowFile.getAbsolutePath(), flowFile.getParent());
				LOG.log(new Status(Status.INFO, BUNDLE, "Resolved file: "
						+ flowWithAbsolutePaths));
				Metaflow.main(new String[] { "-f", flowWithAbsolutePaths });
			} catch (Exception e) {
				e.printStackTrace();
				LOG.log(new Status(Status.ERROR, BUNDLE, e.getMessage(), e));
			}
		}
		monitor.worked(7);
	}

	private String resolveDotInPaths(String flow, String parent)
			throws IOException {
		String resolvedContent = read(flow)
		/* just a dot, in a var: "." or "./" */
		.replaceAll("\"\\./?\"", "\"" + parent + "/\"")
		/* leading dot in a path: "./etc" */
		.replaceAll("\"\\./", "\"" + parent + "/")
		/* somewhat odd case, but supported by Metaflow: */
		.replace("file://./", "file://" + parent + "/");
		return write(resolvedContent).getAbsolutePath();
	}

	private File write(String content) throws IOException {
		File resolvedFile = File.createTempFile("metatext", ".flow");
		resolvedFile.deleteOnExit();
		FileWriter writer = new FileWriter(resolvedFile);
		writer.write(content);
		writer.close();
		return resolvedFile;
	}

	private String read(String flow) throws FileNotFoundException {
		StringBuilder builder = new StringBuilder();
		Scanner scanner = new Scanner(new File(flow));
		while (scanner.hasNextLine()) {
			builder.append(scanner.nextLine()).append("\n");
		}
		scanner.close();
		return builder.toString();
	}

}
