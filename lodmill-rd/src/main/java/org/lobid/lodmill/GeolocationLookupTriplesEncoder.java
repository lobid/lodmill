/* Copyright 2013 Fabian Steeg, Pascal Christoph.
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
	private static final String GEO_WGS84_POS =
			"http://www.w3.org/2003/01/geo/wgs84_pos#";
	private static final String GEO_WGS84_POS_LONG = GEO_WGS84_POS + "long";
	private static final String GEO_WGS84_POS_LAT = GEO_WGS84_POS + "lat";
	private static final String VCARD_NS_LOCALITY =
			"http://www.w3.org/2006/vcard/ns#locality";
	private static final String VCARD_NS_COUNTRY_NAME =
			"http://www.w3.org/2006/vcard/ns#country-name";
	private static final String VCARD_NS_STREET_ADDRESS =
			"http://www.w3.org/2006/vcard/ns#street-address";
	private static final String VCARD_NS_POSTAL_CODE =
			"http://www.w3.org/2006/vcard/ns#postal-code";
	private String bnodeIDGeoPos;
	private String countryName;
	private String locality;
	private String postalcode;
	private String street;
	private HashMap<URL, Double[]> latLon = new HashMap<>();
	private final String latlonFn = "latlon.ser";
	private URL url;
	final String urlOsmLookupFormatParameter = "?format=json";
	private String urlOsmLookupSearchParameters;
	private String urlOsmApiBaseUrl =
			"http://nominatim.openstreetmap.org/search/";
	private Double lat = null;
	private Double lon = null;
	final int urlConnectionTimeOut = 10000; // 10 secs
	private BufferedReader br;
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
			this.countryName = getObjectOfProperty(VCARD_NS_COUNTRY_NAME);
			if ((this.locality = getObjectOfProperty(VCARD_NS_LOCALITY)) != null) {
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
			this.postalcode = getObjectOfProperty(VCARD_NS_POSTAL_CODE);
			this.street = getObjectOfProperty(VCARD_NS_STREET_ADDRESS);
			makeOsmApiUrl();
			if (this.urlOsmLookupSearchParameters != null) {
				lookupLocation(this.urlOsmLookupSearchParameters);
			}
		}
		super.endRecord();
	}

	private String makeOsmApiUrl() {
		if (this.countryName != null && this.locality != null
				&& this.postalcode != null && this.street != null) {
			this.urlOsmLookupSearchParameters =
					String.format("%s/%s/%s/%s", this.countryName, this.locality,
							this.postalcode, this.street);
			return urlOsmApiBaseUrl + urlOsmLookupSearchParameters
					+ urlOsmLookupFormatParameter;
		}
		LOG.warn("One or more parameter needing by the OSM API is missing for "
				+ subject + " : " + this.countryName + "/" + this.locality + "/"
				+ this.postalcode + "/" + this.street);
		return null;
	}

	private boolean makeUrlAndLookupInMap(String urlstr) {
		boolean ret = false;
		try {
			this.url = new URL(urlstr);
		} catch (MalformedURLException e) {
			LOG.error(subject + " " + e.getMessage(), e);
		}
		if (latLon.containsKey(this.urlOsmLookupSearchParameters)) {
			lat = latLon.get(this.urlOsmLookupSearchParameters)[0];
			lon = latLon.get(this.urlOsmLookupSearchParameters)[1];
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
	private void lookupLocation(String urlstr) {
		lat = null;
		lon = null;
		if (!makeUrlAndLookupInMap(urlstr)) {
			try {
				this.br = getUrlContent();
			} catch (IOException e) {
				// ignore, will be treated later
			}
			try {
				parseJsonAndStoreLatLon();
			} catch (Exception e) {
				// "Albertus-Magnus-Pl. 23 (Zimmer 2)" => "Albertus-Magnus-Pl. 23"
				sanitizeStreetnameAndRetrieveOsmApiResult("(.*?\\d+){1}?.*");
				try {
					parseJsonAndStoreLatLon();
				} catch (Exception e1) {
					// "Albertus-Magnus-Pl. 23 (Zimmer 2)" => "Albertus-Magnus-Pl."
					sanitizeStreetnameAndRetrieveOsmApiResult("(.*?){1}\\ .*");
					try {
						parseJsonAndStoreLatLon();
					} catch (Exception e2) {
						// failed definetly
						LOG.error(subject + " URL:" + url + " ," + e2.getLocalizedMessage());
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

	private void sanitizeStreetnameAndRetrieveOsmApiResult(String regex) {
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
			String urlString = makeOsmApiUrl();
			try {
				makeUrlAndLookupInMap(urlString);
				this.br = getUrlContent();
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
	}

	private BufferedReader getUrlContent() throws IOException {
		URLConnection urlConnection = this.url.openConnection();
		urlConnection.setConnectTimeout(this.urlConnectionTimeOut);
		br =
				new BufferedReader(
						new InputStreamReader(urlConnection.getInputStream()));
		return br;
	}

	private String getObjectOfProperty(String pro) {
		NodeIterator it = model.listObjectsOfProperty(model.getProperty(pro));
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
			latLon = (HashMap<URL, Double[]>) ois.readObject();
			System.out.println(latLon.size());
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
		if (this.latLon.size() > 0) {
			try (FileOutputStream fos = new FileOutputStream(latlonFn);
					ObjectOutputStream oos = new ObjectOutputStream(fos)) {
				oos.writeObject(latLon);
				System.out.println("latLon=" + latLon.size());
				oos.close();
			} catch (IOException e) {
				LOG.error(e.getMessage(), e);
			}
		}
	}
}
