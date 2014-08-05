/**
 * 
 */
package mitll.xdata.dataset.bitcoin.ingest;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import mitll.xdata.dataset.bitcoin.binding.BitcoinBinding;
import mitll.xdata.db.DBConnection;
import mitll.xdata.db.H2Connection;

import org.apache.log4j.Logger;

/**
 * Re-creating func
 * 
 * @author ca22119
 * 
 */

/*
//0
alter table transactions_copy add sorted_pair array;
update transactions_copy 
set sorted_pair =
case when (source > target) then (target,source) else (source,target) end;
// 1
drop table if exists sorted_pairs;
create table sorted_pairs as SELECT sorted_pair, sum(usd) as tot_usd, count(*) as num_trans FROM TEST group by sorted_pair;
alter table sorted_pairs add tot_out decimal(20,8);
alter table sorted_pairs add tot_in decimal(20,8);
//2
drop table if exists tmp;
create temporary table tmp as select sorted_pair, sum(usd) as tot_out
from test
where (source < target)
group by sorted_pair;
//3
update sorted_pairs set tot_out =
(select tmp.tot_out
from tmp
where sorted_pairs.sorted_pair = tmp.sorted_pair);
//4
drop table if exists tmp;
create table tmp as select sorted_pair, sum(usd) as tot_in
from test
where (source > target)
group by sorted_pair;
//5
update sorted_pairs set tot_in =
(select tmp.tot_in
from tmp
where sorted_pairs.sorted_pair = tmp.sorted_pair);
//6
drop table tmp;
*/


public class BitcoinSubGraphIngest {
	
	private static final Logger logger = Logger.getLogger(BitcoinSubGraphIngest.class);
	
	private static final int MIN_TRANSACTIONS = 10;
	private static final boolean LIMIT = false;
	private static final int BITCOIN_OUTLIER = 25;
	private static final int USER_LIMIT = 10000000;
	private static final int MIN_DEBorCRED = 5;
	private static final String TABLE_NAME = "transactions_copy";
	
	
	private static PreparedStatement queryStatement;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		logger.info("test");
		
		//String bitcoinDirectory = "src/main/resources/bitcoin_small_feats_tsv/";
	    String bitcoinDirectory = "src/main/resources" + BitcoinBinding.BITCOIN_FEATS_TSV;
		
		DBConnection dbConnection = new H2Connection(bitcoinDirectory,"bitcoin");
		
		filterForActivity(dbConnection);
	}

	/**
	 * Filter out accounts involved in fewer than {@link #MIN_TRANSACTIONS} so that
	 * the transactions table has only transactions involving accounts that transact 
	 * "frequently enough." Also creates a table containing all users and their total
	 * number of transactions, "USERS"
	 *
	 * NOTES:
	 * --Filter out self-to-self transactions
	 * --Throw out "supernode" #25
	 * 
	 * @param connection
	 * @return
	 * @throws Exception
	 */
	private static void filterForActivity(DBConnection connection) throws Exception {
		
		/*
		 * Setup SQL queries
		 */
		String sqlRemoveAccount = "delete from "+TABLE_NAME+" where"+
				" (source = "+BITCOIN_OUTLIER+") or"+ 
				" (target  = "+BITCOIN_OUTLIER+");";
		
		String sqlRemoveSelfTrans = "delete from "+TABLE_NAME+" where (source = target);";

		String sqlFindInactive = 
				"drop table if exists inactive;" +
						" create table inactive as" +
						" select uid, sum(cnt) as tot_count from" + 
						" (select source as uid, count(*) as cnt from " + TABLE_NAME +
						" group by source" +
						" union" +
						" select target as uid, count(*) as cnt from " + TABLE_NAME +
						" group by target order by uid) as tbl" +
						" group by uid having tot_count < "+MIN_TRANSACTIONS+";";
		
		String sqlCountInactive = "select count(*) as cnt from inactive;";
		
		String sqlRemoveInactiveAccounts = "delete from "+TABLE_NAME+" where"+
				" (source in (select distinct uid from inactive)) or"+
				" (target in (select distinct uid from inactive));";
		
		/*
		 * Do some initial clean-up
		 */
		doSQLUpdate(connection, sqlRemoveAccount);
		doSQLUpdate(connection, sqlRemoveSelfTrans);
		
		/*
		 * Find and remove non-active accounts until only active accounts remain in transactions table 
		 */
		int numInactive = 1;
		while (numInactive != 0) {
			//find inactive
			doSQLUpdate(connection, sqlFindInactive); 

			//how many inactive accounts
			ResultSet rs = doSQLQuery(connection, sqlCountInactive); rs.next();
			numInactive = rs.getInt("CNT");
			logger.info("number of inactive acounts in "+TABLE_NAME+": "+numInactive);
			queryStatement.close();			
			
			//remove all inactive accounts
			doSQLUpdate(connection, sqlRemoveInactiveAccounts);
		}
		
		//cleanup
		doSQLUpdate(connection, "drop table inactive");
	}	
	
	/**
	 * Do SQL, update something, return nothing
	 * 
	 * @param connection
	 * @param createSQL
	 * @throws SQLException
	 */
	private static void doSQLUpdate(DBConnection connection, String createSQL) throws SQLException {
		Connection connection1 = connection.getConnection();
		PreparedStatement statement = connection1.prepareStatement(createSQL);
		statement.executeUpdate();
		statement.close();
	}

	/**
	 * Do SQL, query something, return that something
	 * 
	 * @param connection
	 * @param createSQL
	 * @return
	 * @throws SQLException
	 */
	private static ResultSet doSQLQuery(DBConnection connection, String createSQL) throws SQLException {
		Connection connection1 = connection.getConnection();
		//PreparedStatement statement = connection1.prepareStatement(createSQL);
		queryStatement = connection1.prepareStatement(createSQL);
		ResultSet rs = queryStatement.executeQuery();
		//statement.close();
		
		return rs;
	}	
	
}
