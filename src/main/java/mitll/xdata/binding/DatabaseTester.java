/*
 * Copyright 2013-2016 MIT Lincoln Laboratory, Massachusetts Institute of Technology
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * 
 */
package mitll.xdata.binding;

import mitll.xdata.db.DBConnection;
import mitll.xdata.db.H2Connection;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

/**
 * @author ca22119
 *
 */
public class DatabaseTester {
	private static final Logger logger = Logger.getLogger(DatabaseTester.class);
	
	private static final int MIN_TRANSACTIONS = 10;
	private static final boolean LIMIT = false;
	private static final int BITCOIN_OUTLIER = 25;
	private static final int USER_LIMIT = 10000000;
	private static final int MIN_DEBorCRED = 5;
	private static final String TABLE_NAME = "transactions_copy";
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
		logger.info("test");
		
		String bitcoinDirectory = "src/main/resources/bitcoin_small_feats_tsv/";
		
		DBConnection dbConnection = new H2Connection(bitcoinDirectory,"bitcoin");
		
		//String sql = "SELECT * FROM TRANSACTIONS LIMIT 100";
		String sql =
				"select uid, sum(cnt) as tot_count from" +
						" (select source as uid, count(*) as cnt from " + TABLE_NAME +
						" where (source <> " + BITCOIN_OUTLIER + ")" + 
						" and (target <> " + BITCOIN_OUTLIER + ")" +
						" and (source <> target) group by source having cnt > " + MIN_DEBorCRED +
						" union" +
						" select target as uid, count(*) as cnt from "+ TABLE_NAME +
						" where (source <> "+ BITCOIN_OUTLIER +")" +
						" and (target <> "+ BITCOIN_OUTLIER +")" +
						" and (source <> target) group by target having cnt > " + MIN_DEBorCRED + 
						" order by uid) as tbl" + 
						" group by uid order by uid";
		
		logger.info(sql);
		
		PreparedStatement statement = dbConnection.getConnection().prepareStatement(sql);
		ResultSet rs = statement.executeQuery();
		
		//HashMap<Integer, Integer> user2TransCount = new HashMap<Integer, Integer>();
		
		int c=0;
	    while (rs.next()) {
	        c++;
	        if (c % 100000 == 0) {logger.debug("read  " +c);}
	        
	        if (c > 20) {break;}
	        
	         //Retrieve by column name
	         int guid  = rs.getInt("UID");
	         int cnt = rs.getInt("TOT_COUNT");
	         
	         String msg = "UID: " + guid +
	        		 "\tTOT_COUNT: " + cnt;
	         
	         logger.info(msg);
	       
	      }

	      rs.close();
	      statement.close();
		
		dbConnection.closeConnection();
		
	}
	
	private static void doSQL(DBConnection connection, String createSQL) throws SQLException {
		Connection connection1 = connection.getConnection();
		PreparedStatement statement = connection1.prepareStatement(createSQL);
		statement.executeUpdate();
		statement.close();
	}
	
	/**
	 * Get accounts and total num of transactions in which they're involved
	 * --Filter out accounts that have less than {@link #MIN_DEBorCRED} transactions.
	 * --Filter out self-to-self transactions
	 * --Count how many total transactions an account is party to
	 * --NOTE : throws out "supernode" #25
	 * 
	 * @param connection
	 * @return
	 * @throws Exception
	 */
	private HashMap<Integer, Integer> getUser2TransactionCount(DBConnection connection) throws Exception {
		long then = System.currentTimeMillis();

		String sql =
				"select uid, sum(cnt) as tot_count from" +
						" (select source as uid, count(*) as cnt from " + TABLE_NAME +
						" where (source <> " + BITCOIN_OUTLIER + ")" + 
						" and (target <> " + BITCOIN_OUTLIER + ")" +
						" and (source <> target) group by source having cnt > " + MIN_DEBorCRED +
						" union" +
						" select target as uid, count(*) as cnt from "+ TABLE_NAME +
						" where (source <> "+ BITCOIN_OUTLIER +")" +
						" and (target <> "+ BITCOIN_OUTLIER +")" +
						" and (source <> target) group by target having cnt > " + MIN_DEBorCRED + 
						" order by uid) as tbl" + 
						" group by uid order by uid";

		PreparedStatement statement = connection.getConnection().prepareStatement(sql);
		ResultSet rs = statement.executeQuery();
		
		HashMap<Integer, Integer> user2TransCount = new HashMap<Integer, Integer>();
		
		/*
		int c = 0;

		while (rs.next()) {
			c++;
			if (c % 100000 == 0) logger.debug("read  " +c);
			ids.add(rs.getInt(1));
		}
		long now = System.currentTimeMillis();
		logger.debug("took " +(now-then) + " millis to read " + ids.size() + " users");

		rs.close();
		statement.close();
		return  ids;
		 */
		
		return user2TransCount;
	}	

}
