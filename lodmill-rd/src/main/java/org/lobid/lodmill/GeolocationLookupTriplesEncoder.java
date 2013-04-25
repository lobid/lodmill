/* Copyright 2013 hbz, Pascal Christoph
 * Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;

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

import com.hp.hpl.jena.rdf.model.NodeIterator;

/**
 * Lookup in openstreetmap-Web-API to get geo locations. Caches these in
 * serialized HashMap. Encode geo locations in triples.
 * 
 * @author Pascal Christoph
 */
@Description("Lookup geo location data in OSM")
@In(StreamReceiver.class)
@Out(String.class)
public class GeolocationLookupTriplesEncoder extends PipeEncodeTriples {
	private enum VcardNs {
		LOCALITY("http://www.w3.org/2006/vcard/ns#locality"), COUNTRY_NAME(
				"http://www.w3.org/2006/vcard/ns#country-name"), STREET_ADDRESS(
				"http://www.w3.org/2006/vcard/ns#street-address"), POSTAL_CODE(
				"http://www.w3.org/2006/vcard/ns#postal-code");
		String uri;

		VcardNs(String uri) {
			this.uri = uri;
		}
	}

	private static final String GEO_WGS84_POS =
			"http://www.w3.org/2003/01/geo/wgs84_pos#";
	private static final String GEO_WGS84_POS_LONG = GEO_WGS84_POS + "long";
	private static final String GEO_WGS84_POS_LAT = GEO_WGS84_POS + "lat";
	private static final String latlonFn = "src/main/resources/latlon.ser";
	private static final String urlOsmLookupFormatParameter = "?format=json";
	private static final String urlOsmApiBaseUrl =
			"http://nominatim.openstreetmap.org/search/";
	private String urlOsmLookupSearchParameters;
	private String bnodeIDGeoPos;
	private String countryName;
	private String locality;
	private String postalcode;
	private String street;
	private HashMap<String, Double[]> latLon = new HashMap<>();
	private URL url;
	private Double lat = null;
	private Double lon = null;
	private static final int urlConnectionTimeOut = 10000; // 10 secs
	private BufferedReader br;
	private boolean latLonChanged;
	private static final Logger LOG = LoggerFactory
			.getLogger(GeolocationLookupTriplesEncoder.class);

	@Override
	public void literal(final String name, final String value) {
		if (name.startsWith(GEO_WGS84_POS)) {
			this.bnodeIDGeoPos = value;
		}
		super.literal(name, value);
	}

	@Override
	public void endRecord() {
		this.url = null;
		this.urlOsmLookupSearchParameters = null;
		try {
			// if entries already there, do nothing
			Double.valueOf(getObjectOfProperty(GEO_WGS84_POS_LAT));
			Double.valueOf(getObjectOfProperty(GEO_WGS84_POS_LONG));
		} catch (Exception e) {
			this.countryName = getObjectOfProperty(VcardNs.COUNTRY_NAME.uri);
			if ((this.locality = getObjectOfProperty(VcardNs.LOCALITY.uri)) != null) {
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
			this.postalcode = getObjectOfProperty(VcardNs.POSTAL_CODE.uri);
			this.street = getObjectOfProperty(VcardNs.STREET_ADDRESS.uri);
			if (makeOsmApiSearchParameters()) {
				lookupLocation();
			}
		}
		super.endRecord();
	}

	private boolean makeOsmApiSearchParameters() {
		boolean ret = false;
		if (this.countryName != null && this.locality != null
				&& this.postalcode != null && this.street != null) {
			this.urlOsmLookupSearchParameters =
					String.format("%s/%s/%s/%s", this.countryName, this.locality,
							this.postalcode, this.street);
			ret = true;
		} else {
			LOG.warn("One or more parameter needing by the OSM API is missing for "
					+ subject + " : " + this.countryName + "/" + this.locality + "/"
					+ this.postalcode + "/" + this.street);
		}
		return ret;
	}

	private boolean makeUrlAndLookupInMap() {
		boolean ret = false;
		try {
			this.url =
					new URL(urlOsmApiBaseUrl + urlOsmLookupSearchParameters
							+ urlOsmLookupFormatParameter);
		} catch (MalformedURLException e) {
			LOG.error(subject + " " + e.getMessage(), e);
		}
		if (latLon.containsKey(urlOsmLookupSearchParameters)) {
			lat = latLon.get(urlOsmLookupSearchParameters)[0];
			lon = latLon.get(urlOsmLookupSearchParameters)[1];
			ret = true;
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
		if (!makeUrlAndLookupInMap()) {
			try {
				this.br = getUrlContent();
			} catch (IOException e) {
				// ignore, will be treated later
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
						LOG.warn("Could not generate geo location for " + subject
								+ ". The URL is:" + url, e2);
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
					if (!makeUrlAndLookupInMap()) {
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
		latLon.put(this.urlOsmLookupSearchParameters, doubleArr);
		this.latLonChanged = true;
	}

	private BufferedReader getUrlContent() throws IOException {
		URLConnection urlConnection = this.url.openConnection();
		urlConnection.setConnectTimeout(urlConnectionTimeOut);
		br =
				new BufferedReader(
						new InputStreamReader(urlConnection.getInputStream()));
		return br;
	}

	private String getObjectOfProperty(String ns) {
		NodeIterator it = model.listObjectsOfProperty(model.getProperty(ns));
		if (it.hasNext()) {
			try {
				return URIUtil.encodeQuery(it.next().asLiteral().getLexicalForm(),
						"UTF-8");
			} catch (URIException e) {
				LOG.error(subject + " " + e.getMessage(), e);
			}
		}
		return null;
	}

	@Override
	protected void onSetReceiver() {
		super.onSetReceiver();
		// see https://wiki.openstreetmap.org/wiki/DE:Nominatim#Nutzungsbedingungen
		System.setProperty("http.agent",
				"java.net.URLConnection, email=<semweb@hbz-nrw.de>");
		try (FileInputStream fis = new FileInputStream(latlonFn);
				ObjectInputStream ois = new ObjectInputStream(fis)) {
			latLon = (HashMap<String, Double[]>) ois.readObject();
			System.out.println("Number of cached URLs in file " + latlonFn + ":"
					+ latLon.size());
			ois.close();
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		} catch (ClassNotFoundException e) {
			LOG.error(e.getMessage(), e);
		}
	}

	@Override
	protected void onCloseStream() {
		super.onCloseStream();
		if (this.latLon.size() > 0 && latLonChanged) {
			try (FileOutputStream fos = new FileOutputStream(latlonFn);
					ObjectOutputStream oos = new ObjectOutputStream(fos)) {
				oos.writeObject(latLon);
				oos.close();
			} catch (IOException e) {
				LOG.error(e.getMessage(), e);
			}
		}
	}
}
