package org.lobid.lodmill;

import org.culturegraph.mf.morph.Metamorph;
import org.culturegraph.mf.stream.source.DirReader;
import org.culturegraph.mf.stream.source.FileOpener;

/**
 * Builds a concordance of wikidata entities to their labels and their geo
 * coordinates. Dumb parsing of wikidata json entities. Builds a key value
 * structured mysql database.
 * 
 * The source of the entities are single files. See
 * doc/scripts/hbz01/wikidataNrwSettlements.sh for their creation.
 * 
 * @author Pascal Christoph (dr0i)
 * 
 */
public class WikidataGeoJson2Mysql {
	private static final String DB_PROTOCOL_AND_ADDRESS =
			"jdbc:mysql://localhost:3306/";
	private static final String DB_PASSWORD = "tzSblDEUGC1XhJB7";
	private static final String DB_DBNAME = "lobid";
	private static final String WD_SOURCE_PATH =
			"./doc/scripts/hbz01/wikidataEntities/";

	@SuppressWarnings("javadoc")
	public static void main(String... args) {

		final DirReader dirReader = new DirReader();
		final FileOpener opener = new FileOpener();
		dirReader.setReceiver(opener);
		final JsonDecoder jsonOsmDecoder = new JsonDecoder();
		opener.setReceiver(jsonOsmDecoder);
		// parse Wikidata result, get latitude longitude
		final Metamorph morphWD =
				new Metamorph(Thread.currentThread().getContextClassLoader()
						.getResource("morph-wikidataResult-LatLon2Mysql.xml").getFile());
		jsonOsmDecoder.setReceiver(morphWD);
		// writing into SQL DBMS
		MysqlWriter sqlWriter = createMysqlWriter("wikidataGeo");
		morphWD.setReceiver(sqlWriter);
		dirReader.process(WD_SOURCE_PATH);
	}

	private static MysqlWriter createMysqlWriter(String tableName) {
		MysqlWriter sqlWriterOsmUrl = new MysqlWriter();
		sqlWriterOsmUrl.setDbname(DB_DBNAME);
		sqlWriterOsmUrl.setDbProtocolAndAdress(DB_PROTOCOL_AND_ADDRESS);
		sqlWriterOsmUrl.setPassword(DB_PASSWORD);
		sqlWriterOsmUrl.setTablename(tableName);
		sqlWriterOsmUrl.setUsername("debian-sys-maint");
		return sqlWriterOsmUrl;
	}
}
