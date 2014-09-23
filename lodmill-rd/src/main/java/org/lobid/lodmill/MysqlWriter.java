/* Copyright 2014 Pascal Christoph, hbz.
 * Licensed under the Eclipse Public License 1.0 */

package org.lobid.lodmill;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.culturegraph.mf.framework.DefaultStreamPipe;
import org.culturegraph.mf.framework.ObjectReceiver;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A sink, writing objects into MySQL DBMS.
 * 
 * @author Pascal Christoph (dr0i)
 */
@Description("Writes the object value into mysql")
@In(StreamReceiver.class)
@Out(Void.class)
public final class MysqlWriter extends
		DefaultStreamPipe<ObjectReceiver<String>> {
	private static final Logger LOG = LoggerFactory.getLogger(MysqlWriter.class);

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
	public void literal(final String name, final String value) {
		try {
			this.ps.setString(1, name);
			this.ps.setString(2, value);
			this.ps.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// @TODO make Database configs configurable
	private void connectMysqlDB() {
		try {
			String connectionUri =
					this.dbProtocolAndAdress + "?" + "user=" + this.username
							+ "&password=" + this.password;
			LOG.debug("Connection URI =" + connectionUri);
			conn = DriverManager.getConnection(connectionUri);
			stmt = conn.createStatement();
			stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS " + this.dbname);
			conn =
					DriverManager.getConnection(this.dbProtocolAndAdress + this.dbname
							+ "?" + "user=" + this.username + "&password=" + this.password);
			stmt = conn.createStatement();
			stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " + this.tablename + " ( "
					+ this.columnId + " VARCHAR(128), PRIMARY KEY (" + this.columnId
					+ ")," + this.columnData + " VARCHAR(128) ) ENGINE=MYISAM");
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
