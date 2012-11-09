/* Copyright 2012 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package controllers;

import models.Document;
import play.data.Form;
import play.mvc.Controller;
import play.mvc.Result;

/**
 * Main application controller.
 * 
 * @author Fabian Steeg (fsteeg)
 */
public final class Application extends Controller {

	private Application() { // NOPMD
		/* No instantiation */
	}

	private static Form<Document> docForm = form(Document.class);

	public static Result index() {
		return redirect(routes.Application.search());
	}

	public static Result search() {
		return ok(views.html.index.render(Document.all(), docForm));
	}

	public static Result doSearch() {
		final Form<Document> filledForm = docForm.bindFromRequest();
		Result result = null;
		if (filledForm.hasErrors()) {
			result =
					badRequest(views.html.index.render(Document.all(),
							filledForm));
		} else {
			Document.search(filledForm.get());
			result = redirect(routes.Application.search());
		}
		return result;
	}

}