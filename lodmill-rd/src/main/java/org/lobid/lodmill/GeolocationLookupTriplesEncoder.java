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
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;

import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import com.hp.hpl.jena.rdf.model.NodeIterator;

/**
 * Lookup in openstreetmap-Web-API to get geo locations. Caches these in
 * serialized HashMap. Encode geo locations in triples.
 * 
 * 
 * @author Pascal Christoph
 */
@Description("Lookup geo location data in OSM")
@In(StreamReceiver.class)
@Out(String.class)
public class GeolocationLookupTriplesEncoder extends PipeEncodeTriples {
	private String countryName;
	private String locality;
	private String postalcode;
	private String street;
	private HashMap<URL, Double[]> latLon = new HashMap<>();
	private final String latlonFn = "latlon.ser";

	@Override
	public void endRecord() {
		String latString = "http://www.w3.org/2003/01/geo/wgs84_pos#lat";
		String lonString = "http://www.w3.org/2003/01/geo/wgs84_pos#long";
		try {
			Double.valueOf(getObjectOfProperty(latString));
			Double.valueOf(getObjectOfProperty(lonString));
		} catch (Exception e) {

			this.countryName =
					getObjectOfProperty("http://www.w3.org/2006/vcard/ns#country-name");
			this.locality =
					getObjectOfProperty("http://www.w3.org/2006/vcard/ns#locality");
			this.postalcode =
					getObjectOfProperty("http://www.w3.org/2006/vcard/ns#postal-code");
			this.street =
					getObjectOfProperty("http://www.w3.org/2006/vcard/ns#street-address");
			if (this.countryName != null && this.locality != null
					&& this.postalcode != null && this.street != null) {
				String urlString =
						String
								.format(
										"http://nominatim.openstreetmap.org/search/%s/%s/%s/%s?format=json",
										this.countryName, this.locality, this.postalcode,
										this.street);
				lookupLocation(latString, lonString, urlString);
			}
		}
		super.endRecord();
	}

	private void lookupLocation(String latString, String lonString, String urlstr) {
		URL url;
		BufferedReader br;
		String json = null;
		Double lat = null;
		Double lon = null;
		try {
			url = new URL(urlstr);
			if (latLon.containsKey(url)) {
				lat = latLon.get(url)[0];
				lon = latLon.get(url)[1];
			} else {
				br =
						new BufferedReader(new InputStreamReader(url.openConnection()
								.getInputStream()));
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
				Double[] latLonArr = new Double[2];
				latLonArr[0] = lat;
				latLonArr[1] = lon;
				latLon.put(url, latLonArr);
			}
		} catch (MalformedURLException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		super.literal("bnode", "_:pos " + latString + " " + String.valueOf(lat));
		super.literal("bnode", "_:pos " + lonString + " " + String.valueOf(lon));
	}

	private String getObjectOfProperty(String pro) {
		NodeIterator it = model.listObjectsOfProperty(model.getProperty(pro));
		if (it.hasNext()) {
			try {
				return URLEncoder.encode(it.next().asLiteral().getLexicalForm(),
						"UTF-8");
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	@Override
	protected void onSetReceiver() {
		super.onSetReceiver();
		try (FileInputStream fis = new FileInputStream(latlonFn);
				ObjectInputStream ois = new ObjectInputStream(fis)) {
			latLon = (HashMap<URL, Double[]>) ois.readObject();
			ois.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void onCloseStream() {
		super.onCloseStream();
		if (this.latLon.size() > 0) {
			try (FileOutputStream fos = new FileOutputStream(latlonFn);
					ObjectOutputStream oos = new ObjectOutputStream(fos)) {
				oos.writeObject(latLon);
				oos.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
