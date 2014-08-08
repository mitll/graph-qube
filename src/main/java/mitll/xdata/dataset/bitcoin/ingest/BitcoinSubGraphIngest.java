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



public class BitcoinSubGraphIngest {
	
	private static final Logger logger = Logger.getLogger(BitcoinSubGraphIngest.class);
	
	private static final int MIN_TRANSACTIONS = 10;
	private static final boolean LIMIT = false;
	private static final int BITCOIN_OUTLIER = 25;
	private static final int USER_LIMIT = 10000000;
	private static final int MIN_DEBorCRED = 5;
	private static final String TABLE_NAME = "transactions_copy";
	private static final String GRAPH_TABLE = "MARGINAL_GRAPH";
	
	
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
		
		// Filter-out non-active nodes, self-transitions, heavy-hitters
		filterForActivity(dbConnection);
		
		// Create marginalized graph data and various stats
		extractUndirectedGraph(dbConnection);
	}

	
	/**
	 * Find all unique edges and gather statistics about the activity between them.
	 * 
	 * The primary key in this table is the tuple "sorted_pair" which is a guid for each edge 
	 * where the accounts involved in a transaction are listed in numerical order; 
	 * e.g. (a,b) for all transactions between a and b, given a < b. 
	 * 
	 * The column "TOT_USD" sums the total amount transacted between a and b
	 * The column "NUM_TRANS" count the total number of transactions between a and b
	 * The column "TOT_OUT" sums the value of transactions from a->b
	 * The column "TOT_IN" sums the value of transactions from b->a 
	 * 
	 * @param connection
	 * @return
	 * @throws Exception
	 */
	private static void extractUndirectedGraph(DBConnection connection) throws Exception {
		
		/*
		 * Setup SQL statements
		 */
		String sqlGenerateSortedPairs = "alter table "+TABLE_NAME+" add sorted_pair array;" + 
											" update "+TABLE_NAME+ 		
											" set sorted_pair =" +
											" case when (source > target)" +
											" then (target,source) " +
											"else (source,target) end;";
		
		String sqlPopulateMarginalGraph = "drop table if exists "+GRAPH_TABLE+";" +
											" create table "+GRAPH_TABLE+" as " +
											" select sorted_pair, sum(usd) as tot_usd, count(*) as num_trans" +
											" from "+TABLE_NAME+" group by sorted_pair;" +
											" alter table "+GRAPH_TABLE+" add tot_out decimal(20,8);" +
											" alter table "+GRAPH_TABLE+" add tot_in decimal(20,8);";
		
		String sqlTotOutTemp = "drop table if exists tmp;" +
									" create temporary table tmp as select sorted_pair, sum(usd) as tot_out" +
									" from "+TABLE_NAME +
									" where (source < target)" +
									" group by sorted_pair;";

		String sqlUpdateTotOut = "update "+GRAPH_TABLE+" set tot_out =" +
									" (select tmp.tot_out" +
									" from tmp" +
									" where "+GRAPH_TABLE+".sorted_pair = tmp.sorted_pair);";

		String sqlTotInTemp = "drop table if exists tmp;" +
				" create temporary table tmp as select sorted_pair, sum(usd) as tot_in" +
				" from "+TABLE_NAME+
				" where (source > target)" +
				" group by sorted_pair;";

		String sqlUpdateTotIn = "update "+GRAPH_TABLE+" set tot_in =" +
				" (select tmp.tot_in" +
				" from tmp" +
				" where "+GRAPH_TABLE+".sorted_pair = tmp.sorted_pair);";
		
		String sqlDropTemp = "drop table tmp";
		
		doSQLUpdate(connection, sqlGenerateSortedPairs);
		doSQLUpdate(connection, sqlPopulateMarginalGraph);
		doSQLUpdate(connection, sqlTotOutTemp);
		doSQLUpdate(connection, sqlUpdateTotOut);
		doSQLUpdate(connection, sqlTotInTemp);
		doSQLUpdate(connection, sqlUpdateTotIn);
		doSQLUpdate(connection, sqlDropTemp);
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
