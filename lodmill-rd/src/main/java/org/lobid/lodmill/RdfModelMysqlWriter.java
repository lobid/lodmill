/* Copyright 2013 Pascal Christoph, hbz.
 * Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.NoSuchElementException;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.culturegraph.mf.framework.DefaultStreamReceiver;
import org.culturegraph.mf.framework.ObjectReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.LiteralRequiredException;
import com.hp.hpl.jena.rdf.model.Model;

/**
 * A sink, writing triples into MySQL DBMS. The primary keys are constructed
 * from the literals of an property residing in the RDF model.
 * 
 * @author Pascal Christoph
 */
@Description("Writes the object value of an RDF model into MySQL. Default serialization is 'NTRIPLES'. The name of the entry is "
		+ "constructed from the literal of an given property (recommended properties are identifier).\n"
		+ " Mandatory variable are:\n"
		+ "- username\n"
		+ "- password\n"
		+ "- dbname\n"
		+ "- tablename\n"
		+ "- columnId\n"
		+ "- columnData\n"
		+ "\n"
		+ " Optional variables are:\n"
		+ "- 'property' (a property in the RDF model. The object value of this property"
		+ " will be the DB's entry name.) \n"
		+ "- 'serialization (e.g. one of 'NTRIPLES', 'TURTLE', 'RDFXML','RDFJSON'\n"
		+ "- dbProtocolAndAdress\n")
@In(Model.class)
@Out(Void.class)
public final class RdfModelMysqlWriter extends DefaultStreamReceiver implements
		RecordIdentifier, RDFSink, ObjectReceiver<Model> {
	private static final Logger LOG = LoggerFactory
			.getLogger(RdfModelMysqlWriter.class);

	private Lang serialization;
	private String nameProperty;
	private Connection conn = null;
	private Statement stmt = null;
	private PreparedStatement ps;
	private String tablename;
	private final String columnId = "identifier";
	private final String columnData = "data";
	private String dbname;
	private String username;
	private String password;
	private String dbProtocolAndAdress;

	/**
	 * Default constructor
	 * 
	 */
	public RdfModelMysqlWriter() {
		setProperty("http://purl.org/dc/terms/identifier");
		setSerialization("NTRIPLES");
	}

	private void init() {
		if (this.dbProtocolAndAdress != null && this.username != null
				&& this.password != null && this.tablename != null
				&& this.dbname != null) {
			connectMysqlDB();
			try {
				// the "REPLACE" is no standard ANSI SQL, only works with MySQL
				this.ps =
						conn.prepareStatement("REPLACE INTO " + this.tablename + "("
								+ this.columnId + "," + this.columnData + ") VALUES  (?,?)");
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Sets the protocoll and adress of the DBMS, e. g. "jdbc:mysql://localhost/"
	 * 
	 * @param dbProtocolAndAdress the protocol and adress of the DBMS
	 */
	public void setDbProtocolAndAdress(final String dbProtocolAndAdress) {
		this.dbProtocolAndAdress = dbProtocolAndAdress;
		init();
	}

	/**
	 * Sets the username of the DBMS.
	 * 
	 * @param username the name of the user
	 */
	public void setUsername(final String username) {
		this.username = username;
		init();
	}

	/**
	 * Sets the password of the username of the DBMS.
	 * 
	 * @param password the password of the user of the DBMS
	 */
	public void setPassword(final String password) {
		this.password = password;
		init();
	}

	/**
	 * Sets the serialization format. Default is NTriples.
	 * 
	 * @param serialization the serialization of the triples
	 */
	@Override
	public void setSerialization(final String serialization) {
		this.serialization = RDFLanguages.nameToLang(serialization);
	}

	@Override
	public void setProperty(String nameProperty) {
		this.nameProperty = nameProperty;
	}

	/**
	 * Sets the name of the database of the DBMS.
	 * 
	 * @param dbname the name of the database
	 */
	public void setDbname(final String dbname) {
		this.dbname = dbname;
		init();
	}

	/**
	 * Sets the name of the table of the database of the DBMS
	 * 
	 * @param tablename the name of the table
	 */
	public void setTablename(final String tablename) {
		this.tablename = tablename;
		init();
	}

	@Override
	public void process(final Model model) {
		String identifier = null;
		try {
			identifier =
					model.listObjectsOfProperty(model.createProperty(nameProperty))
							.next().asLiteral().toString();
			LOG.debug("Going to store identifier=" + identifier);
		} catch (NoSuchElementException e) {
			LOG.warn("No identifier => cannot derive a filename for "
					+ model.toString());
			return;
		} catch (LiteralRequiredException e) {
			LOG.info("Identifier is a URI. Derive filename from that URI ... "
					+ model.toString(), e);
			identifier =
					model.listObjectsOfProperty(model.createProperty(nameProperty))
							.next().toString();
		}
		if (identifier != null) {
			final StringWriter tripleWriter = new StringWriter();
			RDFDataMgr.write(tripleWriter, model, this.serialization);
			String triples = tripleWriter.getBuffer().toString();
			if (triples != null && triples.length() > 1) {
				try {
					this.ps.setString(1, identifier);
					this.ps.setString(2, triples);
					this.ps.executeUpdate();
				} catch (SQLException e) {
					e.printStackTrace();
					this.ps.toString();
				}
			}
		}
	}

	private void connectMysqlDB() {
		try {
			conn =
					DriverManager.getConnection(this.dbProtocolAndAdress + "?" + "user="
							+ this.username + "&password=" + this.password);
			stmt = conn.createStatement();
			stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS " + this.dbname);
			conn =
					DriverManager.getConnection(this.dbProtocolAndAdress + this.dbname
							+ "?" + "user=" + this.username + "&password=" + this.password);
			stmt = conn.createStatement();
			stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " + this.tablename + " ( "
					+ this.columnId + " VARCHAR(20), PRIMARY KEY (" + this.columnId
					+ ")," + this.columnData + " MEDIUMTEXT) ENGINE = MyISAM");
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException sqlEx) {
					LOG.error("Closing SQL statement Exception:" + sqlEx.getMessage());
				}
				stmt = null;
			}
		} catch (SQLException ex) {
			LOG.error("SQLException: " + ex.getMessage());
			LOG.error("SQLState: " + ex.getSQLState());
			LOG.error("VendorError: " + ex.getErrorCode());
		}
	}

}
