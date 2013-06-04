/* Copyright 2013 hbz, Pascal Christoph
 * Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;

import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.util.URIUtil;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.zxing.WriterException;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.LiteralRequiredException;
import com.hp.hpl.jena.rdf.model.NodeIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.util.ResourceUtils;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;

/**
 * Lookup in openstreetmap-Web-API to get geo locations. Caches these in
 * serialized HashMap. Triplify these geo locations. Lookup in geonames dump and
 * triplify matches. Create qr codes and triplify them.
 * 
 * @TODO instead of doing everything (transformation of zdb-isil-file and
 *       enrichment) in one class it may be better to first transform into
 *       ntriples using @PipeEncodeTriples and use the output file as the input
 *       for another flux chain, serializing the new gained ntriples and merge
 *       the two files in the end. But then, this leads to greater redundancy.
 * 
 * @author Pascal Christoph
 */
@Description("Lookup geo location data in OSM")
@In(StreamReceiver.class)
@Out(String.class)
public class PipeLobidOrganisationEnrichment extends PipeEncodeTriples {
	private static final String HTTP_PURL_ORG_LOBID_LIBTYPE_N86 =
			"http://purl.org/lobid/libtype#n86";
	private static final String HTTP_WWW_W3_ORG_NS_ORG_CLASSIFICATION =
			"http://www.w3.org/ns/org#classification";

	private enum VcardNs {
		LOCALITY("http://www.w3.org/2006/vcard/ns#locality"), COUNTRY_NAME(
				"http://www.w3.org/2006/vcard/ns#country-name"), STREET_ADDRESS(
				"http://www.w3.org/2006/vcard/ns#street-address"), POSTAL_CODE(
				"http://www.w3.org/2006/vcard/ns#postal-code"), EMAIL(
				"http://www.w3.org/2006/vcard/ns#email"), VOICE(
				"http://www.w3.org/2006/vcard/ns#Voice"), HOMEPAGE(
				"http://www.w3.org/2006/vcard/ns#url");
		String uri;

		VcardNs(String uri) {
			this.uri = uri;
		}
	}

	private static final String FOAF_NAME = "http://xmlns.com/foaf/0.1/name";
	private static final String GEO_WGS84_POS =
			"http://www.w3.org/2003/01/geo/wgs84_pos#";
	private static final String GN_LOCATED_IN =
			"http://www.geonames.org/ontology#locatedIn";
	private static final String GEO_WGS84_POS_LONG = GEO_WGS84_POS + "long";
	private static final String GEO_WGS84_POS_LAT = GEO_WGS84_POS + "lat";
	private static final String LAT_LON_FILENAME = "latlon.ser";
	private static final String OSM_LOOKUP_FORMAT_PARAMETER = "format=json";
	private static final String OSM_API_BASE_URL =
			"http://nominatim.openstreetmap.org/search";

	// use two different API parameters, example:
	// [0]="http://nominatim.openstreetmap.org/search.php?q=germany+k%C3%B6ln+50679+library&format=json"
	// [1]="http://nominatim.openstreetmap.org/search/95643/Tirschenreuth/bahnhofstr.?format=json"
	private String[] urlOsmLookupSearchParameters = new String[2];
	private Resource bnodeIDGeoPos;
	private String countryName;
	private String locality;
	private String postalcode;
	private String street;

	private static HashMap<String, Double[]> LAT_LON = new HashMap<>();
	// will be persisted only temporarily
	private static HashSet<String> LAT_LON_LOOKUP_NULL = new HashSet<>();
	private static HashMap<String, Integer> GEONAMES_REGION_ID = new HashMap<>();
	private URL[] osmUrl = new URL[2];
	private Double lat = null;
	private Double lon = null;
	private static final int URL_CONNECTION_TIMEOUT = 10000; // 10 secs
	private BufferedReader osmApiLookupResult;
	private boolean latLonChanged;
	private static final Logger LOG = LoggerFactory
			.getLogger(PipeLobidOrganisationEnrichment.class);
	private static final QREncoder QRENCODER = new QREncoder();
	private static final String QR_FILE_PATH = "media/";
	private static final String LV_CONTACTQR =
			"http://purl.org/lobid/lv#contactqr";
	private static final String RDF_SYNTAX_NS_VALUE =
			"http://www.w3.org/1999/02/22-rdf-syntax-ns#value";
	private static final String NS_GEONAMES = "http://sws.geonames.org/";
	private static final String GEONAMES_DE_FILENAME_SYSTEM_PROPERTY =
			"geonames_de_filename";
	private static final String NS_LOBID = "http://lobid.org/";
	private static final String RDF_SYNTAX_NS_TYPE =
			"http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
	private static final String WGS84_POS_SPATIALTHING =
			"http://www.w3.org/2003/01/geo/wgs84_pos#SpatialThing";

	boolean doApiLookup = false;

	@Override
	public void literal(final String name, final String value) {
		if (value == null) {
			LOG.warn("Value should not be null, ID " + "'" + super.subject + "'");
		} else if (!name.equals("")) {
			boolean isRegionalID = name.startsWith(GN_LOCATED_IN);
			super.literal(name, isRegionalID ? createGeonameLink(value) : value);
		}
	}

	@Override
	public void endRecord() {
		if (super.subject != super.DUMMY_SUBJECT) {
			startOsmLookupEnrichment();
			startQREncodeEnrichment();
			ResourceUtils.renameResource(model.getResource(super.DUMMY_SUBJECT),
					super.subject);
			final StringWriter tripleWriter = new StringWriter();
			RDFDataMgr.write(tripleWriter, model, Lang.TURTLE);
			getReceiver().process(tripleWriter.toString());
		} else {
			LOG.info("Missing ISIL, thus ignoring that record.");
			LOG.debug("Record with missing ISIL:" + model.toString());
		}
	}

	@Override
	protected void onSetReceiver() {
		super.onSetReceiver();
		iniOsmApiLookup();
		iniGeonamesDump();
		File file = new File(QR_FILE_PATH);
		if (!file.exists()) {
			file.mkdir();
		}
		if (System.getProperty("doApiLookup").equals("true")) {
			this.doApiLookup = true;
		}
	}

	@Override
	protected void onCloseStream() {
		super.onCloseStream();
		if (LAT_LON.size() > 0 && latLonChanged) {
			try (FileOutputStream fos = new FileOutputStream(LAT_LON_FILENAME);
					ObjectOutputStream oos = new ObjectOutputStream(fos)) {
				oos.writeObject(LAT_LON);
				oos.close();
			} catch (IOException e) {
				LOG.error(e.getMessage(), e);
			}
		}
	}

	@Override
	public void startEntity(final String name) {
		super.startEntity(name);
		if (name.startsWith(GEO_WGS84_POS)) {
			this.bnodeIDGeoPos = super.resources.peek();
		}
	}

	private String getRdfsvalueOfSubjectHavingObject(final String object) {
		String ret = null;
		Node nodeObject = NodeFactory.createURI(object);
		Graph graph = this.model.getGraph();
		ExtendedIterator<Triple> triples;
		triples = graph.find(Node.ANY, Node.ANY, nodeObject);
		if (triples.hasNext()) {
			triples =
					graph.find(triples.next().getSubject(),
							NodeFactory.createURI(RDF_SYNTAX_NS_VALUE), Node.ANY);
			if (triples.hasNext()) {
				ret = triples.next().getObject().getLiteralLexicalForm();
			}
		}
		return ret;
	}

	private void startQREncodeEnrichment() {
		if (this.postalcode == null || this.street == null || this.locality == null)
			return;
		String qrCodeText = createQrCodeText();
		try {
			String isil = (new URI(super.subject)).getPath().replaceAll("/.*/", "");
			QRENCODER.createQRImage(QR_FILE_PATH + isil, qrCodeText,
					(int) (java.lang.Math.sqrt(qrCodeText.length() * 10) + 20) * 2);
			this.model.add(
					this.model.createResource(super.subject),
					this.model.createProperty(LV_CONTACTQR),
					this.model.asRDFNode(NodeFactory.createURI(NS_LOBID + QR_FILE_PATH
							+ isil + QREncoder.FILE_SUFFIX + "." + QREncoder.FILE_TYPE)));
		} catch (URISyntaxException | WriterException | IOException e) {
			e.printStackTrace();
		}
	}

	private String createQrCodeText() {
		final String name = getFirstLiteralOfProperty(FOAF_NAME);
		String qrCodeText =
				"MECARD:N:" + name + ";" + "ADR:" + this.street + "," + this.locality
						+ "," + this.postalcode;
		Resource email = getFirstResourceOfProperty(VcardNs.EMAIL.uri);
		if (email != null)
			qrCodeText =
					qrCodeText + ";EMAIL:" + email.getURI().replaceAll("mailto:", "");
		String telephone = getRdfsvalueOfSubjectHavingObject(VcardNs.VOICE.uri);
		if (telephone != null)
			qrCodeText = qrCodeText + ";TEL:" + telephone;
		Resource homepage = getFirstResourceOfProperty(VcardNs.HOMEPAGE.uri);
		if (homepage != null)
			qrCodeText = qrCodeText + ";URL:" + homepage;
		qrCodeText = qrCodeText + ";END;";
		return qrCodeText;
	}

	private String createGeonameLink(final String value) {
		String ret = null;
		if (GEONAMES_REGION_ID.containsKey(value)) {
			ret = NS_GEONAMES + GEONAMES_REGION_ID.get(value);
		}
		if (ret == null) {
			LOG.warn(String.format(
					"Could not find geoname entry for value '%s' for subject '%s'",
					value, super.subject));
		}
		return ret;
	}

	private static void iniGeonamesDump() {
		try (Scanner geonamesDump =
				new Scanner(Thread
						.currentThread()
						.getContextClassLoader()
						.getResourceAsStream(
								System.getProperty(GEONAMES_DE_FILENAME_SYSTEM_PROPERTY)))) {
			while (geonamesDump.hasNextLine()) {
				String[] geonameDumpLines = geonamesDump.nextLine().split("\t");
				if (geonameDumpLines[13].matches("\\d+")) {
					String gnRegionalId = geonameDumpLines[13];
					int gnId = Integer.parseInt(geonameDumpLines[0]);
					GEONAMES_REGION_ID.put(gnRegionalId, gnId);
				}
			}
		}
	}

	private static void iniOsmApiLookup() {
		// see https://wiki.openstreetmap.org/wiki/DE:Nominatim#Nutzungsbedingungen
		System.setProperty("http.agent",
				"java.net.URLConnection, email=<semweb@hbz-nrw.de>");
		try (FileInputStream fis = new FileInputStream(LAT_LON_FILENAME);
				ObjectInputStream ois = new ObjectInputStream(fis)) {
			LAT_LON = (HashMap<String, Double[]>) ois.readObject();
			LOG.info("Number of cached URLs in file " + LAT_LON_FILENAME + ":"
					+ LAT_LON.size());
			ois.close();
		} catch (IOException e) {
			LOG.info("File not found, will create a new one if necessary.",
					e.getMessage());
		} catch (ClassNotFoundException e) {
			LOG.error(e.getMessage(), e);
		}
	}

	private void startOsmLookupEnrichment() {
		// activate the geo position bnode
		enterBnode(this.bnodeIDGeoPos);
		for (int i = 0; i < 2; i++) {
			osmUrl[i] = null;
			urlOsmLookupSearchParameters[1] = null;
		}
		if (!doubles()) {
			this.countryName = getFirstLiteralOfProperty(VcardNs.COUNTRY_NAME.uri);
			if ((this.locality = getFirstLiteralOfProperty(VcardNs.LOCALITY.uri)) != null) {
				// OSM Api doesn't like e.g /Marburg%2FLahn/ but accepts /Marburg/.
				// Having also the postcode we will not encounter ambigous cities
				try {
					this.locality =
							URIUtil.encodeQuery((URIUtil.decode(this.locality, "UTF-8")
									.replaceAll("(.*)\\p{Punct}.*", "$1")), "UTF-8");
				} catch (URIException e1) {
					e1.printStackTrace();
				}
			}
			this.postalcode = getFirstLiteralOfProperty(VcardNs.POSTAL_CODE.uri);
			this.street = getFirstLiteralOfProperty(VcardNs.STREET_ADDRESS.uri);
			if (makeOsmApiSearchParameters()) {
				lookupLocation(); // TODO check whats happening if geo data already in
													// source file
			}
		}
		if (this.lat != null && this.lon != null) {
			super.literal(RDF_SYNTAX_NS_TYPE, WGS84_POS_SPATIALTHING);
		}
	}

	private boolean doubles() {
		try {
			Double.valueOf(getFirstLiteralOfProperty(GEO_WGS84_POS_LAT));
			Double.valueOf(getFirstLiteralOfProperty(GEO_WGS84_POS_LONG));
		} catch (Exception e) {
			return false;
		}
		return true;
	}

	private boolean makeOsmApiSearchParameters() {
		boolean ret = false;
		if (this.countryName != null && this.locality != null
				&& this.postalcode != null) {
			String osmSearchType = getOsmApiSearchType();
			if (osmSearchType != null) {
				this.urlOsmLookupSearchParameters[0] =
						String.format(osmSearchType + "+%s+%s", this.postalcode,
								this.locality);
			}
			if (this.street != null) {
				this.urlOsmLookupSearchParameters[1] =
						String.format("%s/%s/%s/%s", this.countryName, this.locality,
								this.postalcode, this.street);
				ret = true;
			}
		} else {
			LOG.warn("One or more parameter needing by the OSM API is missing for "
					+ super.subject + " : country=" + this.countryName + ",locality="
					+ this.locality + ",postcode=" + this.postalcode);
		}
		return ret;
	}

	private String getOsmApiSearchType() throws NumberFormatException {
		String OSM_SEARCH_TYPE = null;
		String type;
		Resource res_type =
				getFirstResourceOfProperty(HTTP_WWW_W3_ORG_NS_ORG_CLASSIFICATION);
		if (res_type != null) {
			type = res_type.toString();
			if (Integer.parseInt(type.replaceAll(".*#n", "")) < 85) {
				OSM_SEARCH_TYPE = "library";
			} else if (type.equals(HTTP_PURL_ORG_LOBID_LIBTYPE_N86)) {
				OSM_SEARCH_TYPE = "museum";
			}
		}
		return OSM_SEARCH_TYPE;
	}

	/**
	 * 
	 * @return true if cached, otherwise false
	 */
	private boolean makeUrlAndLookupIfCached() {
		boolean ret = false;
		try {
			osmUrl[0] =
					new URL(OSM_API_BASE_URL + ".php?q="
							+ this.urlOsmLookupSearchParameters[0] + "&"
							+ OSM_LOOKUP_FORMAT_PARAMETER);
			osmUrl[1] =
					new URL(OSM_API_BASE_URL + "/" + this.urlOsmLookupSearchParameters[1]
							+ "?" + OSM_LOOKUP_FORMAT_PARAMETER);
		} catch (MalformedURLException e) {
			LOG.error(super.subject + " " + e.getMessage(), e);
		}
		for (int i = 0; i < 2; i++) {
			if (LAT_LON.containsKey(this.urlOsmLookupSearchParameters[i])) {
				this.lat = LAT_LON.get(this.urlOsmLookupSearchParameters[i])[0];
				this.lon = LAT_LON.get(this.urlOsmLookupSearchParameters[i])[1];
				ret = true;
			}
		}
		if (LAT_LON_LOOKUP_NULL.contains(this.urlOsmLookupSearchParameters[0])
				&& LAT_LON_LOOKUP_NULL.contains(this.urlOsmLookupSearchParameters[1])) {
			LOG.warn("Could not generate geo location for " + super.subject
					+ ". The URL is:" + this.osmUrl[1]);
			ret = true; // do not store anything
		}
		return ret;
	}

	/**
	 * Lookup URL. If no result, make streetname ever more abstract till something
	 * is (hopefully) found via the OSM-API.
	 * 
	 * @param regex
	 */
	private void lookupLocation() {
		this.lat = null;
		this.lon = null;
		// TODO don't use exceptions as control structures
		if (!makeUrlAndLookupIfCached() && this.doApiLookup) {
			try {
				this.osmApiLookupResult = getUrlContent(this.osmUrl[0]);
			} catch (IOException e) {
				// ignore, will be treated below
			}
			try {
				parseJsonAndStoreLatLon();
			} catch (Exception e) {
				try {
					this.osmApiLookupResult = getUrlContent(this.osmUrl[1]);
				} catch (IOException e3) {
					// ignore, will be treated below
				}
				try {
					parseJsonAndStoreLatLon();
				} catch (Exception e3) {
					try {
						// "Albertus-Magnus-Pl. 23 (Zimmer 2)" => "Albertus-Magnus-Pl. 23"
						sanitizeStreetnameAndRetrieveOsmApiResultAndStoreLatLon("(.*?\\d+){1}?.*");
					} catch (Exception e1) {
						try {
							// "Albertus-Magnus-Pl. 23 (Zimmer 2)" => "Albertus-Magnus-Pl."
							sanitizeStreetnameAndRetrieveOsmApiResultAndStoreLatLon("(.*?){1}\\ .*");
						} catch (Exception e2) {
							// failed definetly
							LOG.warn("Failed to generate geo location for " + super.subject
									+ ". The URL is:" + this.osmUrl[1]);
							LAT_LON_LOOKUP_NULL.add(this.urlOsmLookupSearchParameters[1]);
						}
					}
				}
			}
		}
		if (this.lat != null && this.lon != null) {
			super.literal(GEO_WGS84_POS_LAT, String.valueOf(this.lat));
			super.literal(GEO_WGS84_POS_LONG, String.valueOf(this.lon));
		}
	}

	private void sanitizeStreetnameAndRetrieveOsmApiResultAndStoreLatLon(
			String regex) throws Exception {
		String tmp = "";
		try {
			tmp =
					URIUtil.encodeQuery(
							(URIUtil.decode(this.street, "UTF-8").replaceAll(regex, "$1")),
							"UTF-8");
		} catch (URIException e2) {
			e2.printStackTrace();
		}
		// make new request only if strings differ
		if (!tmp.equals(this.street)) {
			this.street = tmp;
			try {
				if (makeOsmApiSearchParameters()) {
					if (!makeUrlAndLookupIfCached()) {
						this.osmApiLookupResult = getUrlContent(this.osmUrl[1]);
						parseJsonAndStoreLatLon();
					}
				}
			} catch (IOException e1) {
				LOG.error(super.subject + " " + e1.getLocalizedMessage());
			}
		}
	}

	private void parseJsonAndStoreLatLon() throws Exception {
		String json;
		StringBuilder builder = new StringBuilder();
		String aux;
		while ((aux = this.osmApiLookupResult.readLine()) != null) {
			builder.append(aux);
		}
		json = builder.toString();
		Object obj = JSONValue.parse(json);
		JSONArray osm = (JSONArray) obj;
		// ignore library search results if result > 1
		// this may lead to wrong results, though, e. g. if there are two libraries
		// of which just one is tagged as library. See
		// http://lobid.org/organisation/DE-Tir1 (where the geo location is in fact
		// of http://lobid.org/organisation/DE-1445 )
		if (osm.size() > 1 && this.osmUrl.toString().contains("library")) {
			LOG.info("More than 1 result for " + super.subject + ", search "
					+ this.osmUrl);
			throw new Exception();
		}
		JSONObject jo = (JSONObject) osm.get(0);
		this.lat = Double.valueOf(jo.get("lat").toString());
		this.lon = Double.valueOf(jo.get("lon").toString());
		Double doubleArr[] = new Double[2];
		doubleArr[0] = this.lat;
		doubleArr[1] = this.lon;
		LAT_LON.put(this.urlOsmLookupSearchParameters[1], doubleArr);
		this.latLonChanged = true;
	}

	private static BufferedReader getUrlContent(final URL url) throws IOException {
		URLConnection urlConnection = url.openConnection();
		urlConnection.setConnectTimeout(URL_CONNECTION_TIMEOUT);
		LOG.debug("Lookup url:" + url);
		return new BufferedReader(new InputStreamReader(
				urlConnection.getInputStream()));
	}

	private String getFirstLiteralOfProperty(String ns) {
		NodeIterator it =
				this.model.listObjectsOfProperty(this.model.getProperty(ns));
		if (it.hasNext()) {
			try {
				return URIUtil.encodeQuery(it.next().asLiteral().getLexicalForm(),
						"UTF-8");
			} catch (URIException e) {
				LOG.error(super.subject + " " + e.getMessage(), e);
			} catch (LiteralRequiredException le) {
				LOG.warn(le.getMessage(), le);
			}
		}
		return null;
	}

	private Resource getFirstResourceOfProperty(String ns) {
		NodeIterator it =
				this.model.listObjectsOfProperty(this.model.getProperty(ns));
		Resource res = null;
		try {
			if (it.hasNext()) {
				res = it.next().asResource();
			}
		} catch (Exception e) {
			LOG.warn("Exception with subject" + super.subject + " Resource=" + res,
					e.getLocalizedMessage());
		}
		return res;
	}

}
