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
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;

import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.util.URIUtil;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.LiteralRequiredException;
import com.hp.hpl.jena.rdf.model.NodeIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;

/**
 * Lookup in openstreetmap-Web-API to get geo locations. Caches these in
 * serialized HashMap. Triplify these geo locations. Lookup in geonames dump and
 * triplify matches. Create qr codes and triplify them.
 * 
 * @TODO instead of doing everything (transformation of zdb-isil-file and
 *       enrichment) in one class it may be better to first transform into
 *       ntriples using @PipeEncodeTriples . Then this file could be used as
 *       input for another flux chain, serializing the new gained ntriples and
 *       merge the two files in the end. But then, this leads to great
 *       redundancy.
 * 
 * @author Pascal Christoph
 */
@Description("Lookup geo location data in OSM")
@In(StreamReceiver.class)
@Out(String.class)
public class PipeLobidOrganisationEnrichment extends PipeEncodeTriples {
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
	private static final String OSM_LOOKUP_FORMAT_PARAMETER = "?format=json";
	private static final String OSM_API_BASE_URL =
			"http://nominatim.openstreetmap.org/search/";
	private String urlOsmLookupSearchParameters;
	private String bnodeIDGeoPos;
	private String countryName;
	private String locality;
	private String postalcode;
	private String street;

	private static HashMap<String, Double[]> LAT_LON = new HashMap<>();
	// will be persisted only temporarily
	private static HashSet<String> LAT_LON_LOOKUP_NULL = new HashSet<>();
	private static HashMap<String, Integer> GEONAMES_REGION_ID = new HashMap<>();
	private URL url;
	private Double lat = null;
	private Double lon = null;
	private static final int URL_CONNECTION_TIMEOUT = 10000; // 10 secs
	private BufferedReader br;
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

	@Override
	public void literal(final String name, String value) {
		if (name.startsWith(GEO_WGS84_POS)) {
			this.bnodeIDGeoPos = value;
		}
		if (value != null) {
			boolean isRegionalID = name.startsWith(GN_LOCATED_IN);
			super.literal(name, isRegionalID ? createGeonameLink(value) : value);
		}
	}

	@Override
	public void endRecord() {
		startOsmLookupEnrichment();
		startQREncodeEnrichment();
		super.endRecord();
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

	private String getRdfsvalueOfSubjectHavingObject(final String object) {
		String ret = null;
		Node nodeObject = Node.createURI(object);
		Graph graph = model.getGraph();
		ExtendedIterator<Triple> triples;
		triples = graph.find(Node.ANY, Node.ANY, nodeObject);
		if (triples.hasNext()) {
			triples =
					graph.find(triples.next().getSubject(),
							Node.createURI(RDF_SYNTAX_NS_VALUE), Node.ANY);
			if (triples.hasNext()) {
				ret = triples.next().getObject().getLiteralLexicalForm();
			}
		}
		return ret;
	}

	private void startQREncodeEnrichment() {
		/*
		 * these are the mandatory variables to create an qr-code
		 */
		final String name = getFirstLiteralOfProperty(FOAF_NAME);
		if (postalcode != null && street != null && locality != null) {
			String qrCodeText =
					"MECARD:N:" + name + ";" + "ADR:" + street + "," + locality + ","
							+ this.postalcode;
			Resource email = getFirstResourceOfProperty(VcardNs.EMAIL.uri);
			if (email != null) {
				qrCodeText =
						qrCodeText + ";EMAIL:" + email.getURI().replaceAll("mailto:", "");
			}
			String telephone = getRdfsvalueOfSubjectHavingObject(VcardNs.VOICE.uri);
			if (telephone != null)
				qrCodeText = qrCodeText + ";TEL:" + telephone;
			Resource homepage = getFirstResourceOfProperty(VcardNs.HOMEPAGE.uri);
			if (homepage != null) {
				qrCodeText = qrCodeText + ";URL:" + homepage;
			}
			qrCodeText = qrCodeText + ";END;";
			try {
				String isil = (new URI(subject)).getPath().replaceAll("/.*/", "");
				QRENCODER.createQRImage(QR_FILE_PATH + isil, qrCodeText,
						(int) (java.lang.Math.sqrt(qrCodeText.length() * 10) + 20) * 2);
				model.add(
						model.createResource(subject),
						model.createProperty(LV_CONTACTQR),
						model.asRDFNode(Node.createURI(NS_LOBID + QR_FILE_PATH + isil
								+ QREncoder.fileSuffix + "." + QREncoder.fileType)));

			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}

	private static String createGeonameLink(final String value) {
		String ret = null;
		if (GEONAMES_REGION_ID.containsKey(value)) {
			ret = NS_GEONAMES + GEONAMES_REGION_ID.get(value);
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
			System.out.println("Number of cached URLs in file " + LAT_LON_FILENAME
					+ ":" + LAT_LON.size());
			ois.close();
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		} catch (ClassNotFoundException e) {
			LOG.error(e.getMessage(), e);
		}
	}

	private void startOsmLookupEnrichment() {
		url = null;
		urlOsmLookupSearchParameters = null;
		try {
			// if entries already there, do nothing
			Double.valueOf(getFirstLiteralOfProperty(GEO_WGS84_POS_LAT));
			Double.valueOf(getFirstLiteralOfProperty(GEO_WGS84_POS_LONG));
		} catch (Exception e) {
			countryName = getFirstLiteralOfProperty(VcardNs.COUNTRY_NAME.uri);
			if ((locality = getFirstLiteralOfProperty(VcardNs.LOCALITY.uri)) != null) {
				// OSM Api doesn't like e.g /Marburg%2FLahn/ but accepts /Marburg/.
				// Having also the postcode we will not encounter ambigous cities
				try {
					locality =
							URIUtil.encodeQuery((URIUtil.decode(locality, "UTF-8")
									.replaceAll("(.*)\\p{Punct}.*", "$1")), "UTF-8");
				} catch (URIException e1) {
					e1.printStackTrace();
				}
			}
			postalcode = getFirstLiteralOfProperty(VcardNs.POSTAL_CODE.uri);
			street = getFirstLiteralOfProperty(VcardNs.STREET_ADDRESS.uri);
			if (makeOsmApiSearchParameters()) {
				lookupLocation();
			}
		}
	}

	private boolean makeOsmApiSearchParameters() {
		boolean ret = false;
		if (countryName != null && locality != null && postalcode != null
				&& street != null) {
			urlOsmLookupSearchParameters =
					String.format("%s/%s/%s/%s", countryName, locality, postalcode,
							street);
			ret = true;
		} else {
			LOG.warn("One or more parameter needing by the OSM API is missing for "
					+ subject + " : " + countryName + "/" + locality + "/" + postalcode
					+ "/" + street);
		}
		return ret;
	}

	private boolean isCached() {
		boolean ret = false;
		try {
			url =
					new URL(OSM_API_BASE_URL + urlOsmLookupSearchParameters
							+ OSM_LOOKUP_FORMAT_PARAMETER);
		} catch (MalformedURLException e) {
			LOG.error(subject + " " + e.getMessage(), e);
		}
		if (LAT_LON.containsKey(urlOsmLookupSearchParameters)) {
			lat = LAT_LON.get(urlOsmLookupSearchParameters)[0];
			lon = LAT_LON.get(urlOsmLookupSearchParameters)[1];
			ret = true;
		} else {
			if (LAT_LON_LOOKUP_NULL.contains(urlOsmLookupSearchParameters)) {
				LOG.warn("Could not generate geo location for " + subject
						+ ". The URL is:" + url);
				ret = true; // do not store anything
			}
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
		lat = null;
		lon = null;
		if (!isCached()) {
			try {
				this.br = getUrlContent();
			} catch (IOException e) {
				// ignore, will be treated later, see below
			}
			try {
				parseJsonAndStoreLatLon();
			} catch (Exception e) {
				try {
					// "Albertus-Magnus-Pl. 23 (Zimmer 2)" => "Albertus-Magnus-Pl. 23"
					sanitizeStreetnameAndRetrieveOsmApiResultAndStoreLatLon("(.*?\\d+){1}?.*");
				} catch (Exception e1) {
					try {
						// "Albertus-Magnus-Pl. 23 (Zimmer 2)" => "Albertus-Magnus-Pl."
						sanitizeStreetnameAndRetrieveOsmApiResultAndStoreLatLon("(.*?){1}\\ .*");
					} catch (Exception e2) {
						// failed definetly
						LOG.warn("Failed to generate geo location for " + subject
								+ ". The URL is:" + url);
						LAT_LON_LOOKUP_NULL.add(urlOsmLookupSearchParameters);
					}
				}
			}
		}
		if (lat != null && lon != null) {
			super.literal("bnode", bnodeIDGeoPos + " " + GEO_WGS84_POS_LAT + " "
					+ String.valueOf(lat));
			super.literal("bnode", bnodeIDGeoPos + " " + GEO_WGS84_POS_LONG + " "
					+ String.valueOf(lon));
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
					if (!isCached()) {
						this.br = getUrlContent();
						parseJsonAndStoreLatLon();
					}
				}
			} catch (IOException e1) {
				LOG.error(subject + " " + e1.getLocalizedMessage());
			}
		}
	}

	private void parseJsonAndStoreLatLon() throws Exception {
		String json;
		StringBuilder builder = new StringBuilder();
		String aux;
		while ((aux = br.readLine()) != null) {
			builder.append(aux);
		}
		json = builder.toString();
		Object obj = JSONValue.parse(json);
		JSONArray osm = (JSONArray) obj;
		JSONObject jo = (JSONObject) osm.get(0);
		lat = Double.valueOf(jo.get("lat").toString());
		lon = Double.valueOf(jo.get("lon").toString());
		Double doubleArr[] = new Double[2];
		doubleArr[0] = lat;
		doubleArr[1] = lon;
		LAT_LON.put(this.urlOsmLookupSearchParameters, doubleArr);
		this.latLonChanged = true;
	}

	private BufferedReader getUrlContent() throws IOException {
		URLConnection urlConnection = this.url.openConnection();
		urlConnection.setConnectTimeout(URL_CONNECTION_TIMEOUT);
		br =
				new BufferedReader(
						new InputStreamReader(urlConnection.getInputStream()));
		return br;
	}

	private String getFirstLiteralOfProperty(String ns) {
		NodeIterator it = model.listObjectsOfProperty(model.getProperty(ns));
		if (it.hasNext()) {
			try {
				return URIUtil.encodeQuery(it.next().asLiteral().getLexicalForm(),
						"UTF-8");
			} catch (URIException e) {
				LOG.error(subject + " " + e.getMessage(), e);
			} catch (LiteralRequiredException le) {
				LOG.warn(le.getMessage(), le);
			}
		}
		return null;
	}

	private Resource getFirstResourceOfProperty(String ns) {
		NodeIterator it = model.listObjectsOfProperty(model.getProperty(ns));
		Resource res = null;
		try {
			if (it.hasNext()) {
				res = it.next().asResource();
			}
		} catch (Exception e) {
			LOG.warn("Exception with subject" + subject + " Resource=" + res,
					e.getLocalizedMessage());
		}
		return res;
	}

}
