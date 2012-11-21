/* Copyright 2012 Fabian Steeg. Licensed under the Eclipse Public License 1.0 */

package controllers;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import models.Document;
import play.data.Form;
import play.libs.Json;
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
	public static final List<String> INDEXES = new ArrayList<String>(
			new TreeSet<String>(Document.searchFieldsMap.keySet()));
	private static int selectedIndex = INDEXES.indexOf(Document.esIndex);
	private static String query = "";

	public static Result index() {
		return redirect(routes.Application.search());
	}

	public static Result search() {
		return ok(views.html.index.render(Document.all(), docForm,
				selectedIndex, query));
	}

	public static Result setIndex() {
		final String index =
				request().body().asFormUrlEncoded().get("index")[0];
		selectedIndex = INDEXES.indexOf(index);
		Document.esIndex = index;
		return redirect(routes.Application.search());
	}

	public static Result doSearch() {
		final Form<Document> filledForm = docForm.bindFromRequest();
		query = request().body().asFormUrlEncoded().get("author")[0];
		Result result = null;
		if (filledForm.hasErrors()) {
			result =
					badRequest(views.html.index.render(Document.all(),
							filledForm, selectedIndex, query));
		} else {
			Document.search(filledForm.get());
			result = redirect(routes.Application.search());
		}
		return result;
	}

	public static Result autocompleteSearch(final String term) {
		final Set<String> set = new HashSet<String>();
		for (Document document : Document.search(term, Document.esIndex)) {
			set.add(document.author);
		}
		return ok(Json.toJson(set));
	}

}