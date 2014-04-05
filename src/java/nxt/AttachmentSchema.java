package nxt;

import nxt.util.Convert;
import nxt.util.DbIterator;
import nxt.util.Logger;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public final class AttachmentSchema {
    private static void apply(String sql) {
        try (Connection con = Db.getConnection()) {
	    apply(con,sql,true);
        } catch (SQLException e) {
            throw new RuntimeException("Database error executing " + sql, e);
        }
    }

    private static void apply(Connection con, String sql, Boolean commitUpdate) {
        try (Statement stmt = con.createStatement()) {
            try {
                if (sql != null) {
                    Logger.logDebugMessage("Will " + (commitUpdate ? "send" : "apply") + " sql:\n" + sql);
                    stmt.executeUpdate(sql);
                }
		
		if (commitUpdate) {
                    Logger.logDebugMessage("Commit");
		    // stmt.executeUpdate("UPDATE version SET next_update = (SELECT next_update + 1 FROM version)");
		    con.commit();
		}
            } catch (SQLException e) {
		Logger.logDebugMessage("Rollback");
                con.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Database error executing " + sql, e);
        }
    }

    private static void drop(Connection con, Boolean commitUpdate) {

	// Create attachment schema
	apply(con,"DROP SCHEMA IF EXISTS attachment",commitUpdate);
    }
	
    private static void create(Connection con, Boolean commitUpdate) {

	// Create attachment schema
	apply(con,"CREATE SCHEMA IF NOT EXISTS attachment",false);

	// In these tables, more could be done with foreign keys. For
	// example: In addition to pointing transaction_id to
	// transaction(id), one could also point
	// (transaction_id,type,subtype) to
	// transaction(id,type,subtype), where type and subtype of the
	// attachment tables would be columns with default values set
	// to the expected type and subtype.  I haven't done this as
	// this would add keys and columns and therefore size to the
	// blockchain DB, but this could be done if one wants the
	// database to do more to be self validating.

	// Also, these tables have no redundant columns, such as
	// sender_id or recipient_id.  I personally prefer to use
	// views with appropriate indexing of join columns instead of
	// adding redundant columns (less redundancy, easier DB
	// maintenance), but they could easily added if desired.
	
	// Create table for messages
	apply(con,
	      "CREATE TABLE IF NOT EXISTS attachment.messaging_arbitrary_message (" +
	      "transaction_id BIGINT NOT NULL PRIMARY KEY " +
	      ", message VARBINARY NOT NULL" +
	      ", FOREIGN KEY (transaction_id) REFERENCES public.transaction(id)" + 
	      " ON DELETE CASCADE ON UPDATE CASCADE" +
	      ")",
	      false);
	
	// Create table for aliases
	apply(con,
	      "CREATE TABLE IF NOT EXISTS attachment.messaging_alias_assignment (" +
	      "transaction_id BIGINT NOT NULL PRIMARY KEY" +
	      ", name VARCHAR NOT NULL" +
	      ", uri VARCHAR" +
	      ", FOREIGN KEY (transaction_id) REFERENCES public.transaction(id)" + 
	      " ON DELETE CASCADE ON UPDATE CASCADE" +
	      ")",
	      false);
	
	// Create table for poll creation
	apply(con,
	      "CREATE TABLE IF NOT EXISTS attachment.messaging_poll_creation (" +
	      "transaction_id BIGINT NOT NULL PRIMARY KEY" +
	      ", name VARCHAR NOT NULL" +
	      ", description VARCHAR" +
	      ", options ARRAY" +
	      ", min_number_of_options TINYINT" +
	      ", max_number_of_options TINYINT" +
	      ", options_are_binary BOOLEAN" +
	      ", FOREIGN KEY (transaction_id) REFERENCES public.transaction(id)" + 
	      " ON DELETE CASCADE ON UPDATE CASCADE" +
	      ")",
	      false);
	
	// Create table for poll vote
	apply(con,
	      "CREATE TABLE IF NOT EXISTS attachment.messaging_vote_casting (" +
	      "transaction_id BIGINT NOT NULL PRIMARY KEY" +
	      ", poll_id BIGINT NOT NULL" +
	      ", vote VARBINARY" +
	      ", FOREIGN KEY (transaction_id) REFERENCES public.transaction(id)" + 
	      " ON DELETE CASCADE ON UPDATE CASCADE" +
	      ", FOREIGN KEY (poll_id) REFERENCES attachment.messaging_poll_creation(transaction_id)" + 
	      " ON DELETE CASCADE ON UPDATE CASCADE" +
	      ")",
	      false);
	
	// Create table for asset issuance
	apply(con,
	      "CREATE TABLE IF NOT EXISTS attachment.colored_coins_asset_issuance (" +
	      "transaction_id BIGINT NOT NULL PRIMARY KEY" +
	      ", name VARCHAR NOT NULL" +
	      ", description VARCHAR" +
	      ", quantity INT NOT NULL" +
	      ", FOREIGN KEY (transaction_id) REFERENCES public.transaction(id)" + 
	      " ON DELETE CASCADE ON UPDATE CASCADE" +
	      ")",
	      false);
	
	// Create table for asset transfer
	apply(con,
	      "CREATE TABLE IF NOT EXISTS attachment.colored_coins_asset_transfer (" +
	      "transaction_id BIGINT NOT NULL PRIMARY KEY" +
	      ", asset BIGINT NOT NULL" +
	      ", quantity INT NOT NULL" +
	      ", comment VARCHAR NOT NULL" +
	      ", FOREIGN KEY (transaction_id) REFERENCES public.transaction(id)" + 
	      " ON DELETE CASCADE ON UPDATE CASCADE" +
	      // As far as I can tell, asset should point to an entry in asset_issuance
	      ", FOREIGN KEY (asset) REFERENCES attachment.colored_coins_asset_issuance(transaction_id)" + 
	      " ON DELETE CASCADE ON UPDATE CASCADE" +
	      ")",
	      false);
	
	// Create table for asset ask order placement
	// Could potentially be combined with bid order placement adding a type column
	apply(con,
	      "CREATE TABLE IF NOT EXISTS attachment.colored_coins_ask_order_placement (" +
	      "transaction_id BIGINT NOT NULL PRIMARY KEY" +
	      ", asset BIGINT NOT NULL" +
	      ", quantity INT NOT NULL" +
	      ", price BIGINT NOT NULL" +
	      ", FOREIGN KEY (transaction_id) REFERENCES public.transaction(id)" + 
	      " ON DELETE CASCADE ON UPDATE CASCADE" +
	      // As far as I can tell, asset should point to an entry in asset_issuance
	      ", FOREIGN KEY (asset) REFERENCES attachment.colored_coins_asset_issuance(transaction_id)" + 
	      " ON DELETE CASCADE ON UPDATE CASCADE" +
	      ")",
	      false);
	
	// Create table for asset bid order placement
	// Could potentially be combined with ask order placement adding a type column
	apply(con,
	      "CREATE TABLE IF NOT EXISTS attachment.colored_coins_bid_order_placement (" +
	      "transaction_id BIGINT NOT NULL PRIMARY KEY" +
	      ", asset BIGINT NOT NULL" +
	      ", quantity INT NOT NULL" +
	      ", price BIGINT NOT NULL" +
	      ", FOREIGN KEY (transaction_id) REFERENCES public.transaction(id)" + 
	      " ON DELETE CASCADE ON UPDATE CASCADE" +
	      // As far as I can tell, asset should point to an entry in asset_issuance
	      ", FOREIGN KEY (asset) REFERENCES attachment.colored_coins_asset_issuance(transaction_id)" + 
	      " ON DELETE CASCADE ON UPDATE CASCADE" +
	      ")",
	      false);

	// Create table for ask cancel order placement
	// Could potentially be combined with bid order cancellation adding a type column
	apply(con,
	      "CREATE TABLE IF NOT EXISTS attachment.colored_coins_ask_order_cancellation (" +
	      "transaction_id BIGINT NOT NULL PRIMARY KEY" +
	      ", order_id BIGINT NOT NULL" +
	      ", FOREIGN KEY (transaction_id) REFERENCES public.transaction(id)" + 
	      " ON DELETE CASCADE ON UPDATE CASCADE" +
	      ", FOREIGN KEY (order_id) REFERENCES attachment.colored_coins_ask_order_placement(transaction_id)" + 
	      " ON DELETE CASCADE ON UPDATE CASCADE" +
	      ")",
	      false);
	
	// Create table for big cancel order placement
	// Could potentially be combined with ask order cancellation adding a type column
	apply(con,
	      "CREATE TABLE IF NOT EXISTS attachment.colored_coins_bid_order_cancellation (" +
	      "transaction_id BIGINT NOT NULL PRIMARY KEY" +
	      ", order_id BIGINT NOT NULL" +
	      ", FOREIGN KEY (transaction_id) REFERENCES public.transaction(id)" + 
	      " ON DELETE CASCADE ON UPDATE CASCADE" +
	      ", FOREIGN KEY (order_id) REFERENCES attachment.colored_coins_bid_order_placement(transaction_id)" + 
	      " ON DELETE CASCADE ON UPDATE CASCADE" +
	      ")",
	      false);

	// One commit for all if desired
	apply(con,null,commitUpdate);
	
    }

    private static void truncate() {
	// Truncate is faster than delete, but cannot be rolled back
	// Using this may play havoc with commit/rollback if same database connection used
	// In general don't use truncate in middle of db transaction

	// Truncate table for messages
	apply("TRUNCATE TABLE attachment.messaging_arbitrary_message");
	
	// Truncate table for aliases
	apply("TRUNCATE TABLE attachment.messaging_alias_assignment");
	
	// Truncate table for poll creation
	apply("TRUNCATE TABLE attachment.messaging_poll_creation");
	
	// Truncate table for poll vote
	apply("TRUNCATE TABLE attachment.messaging_vote_casting");
	
	// Truncate table for asset issuance
	apply("TRUNCATE TABLE attachment.colored_coins_asset_issuance");
	
	// Truncate table for asset transfer
	apply("TRUNCATE TABLE attachment.colored_coins_asset_transfer");
	
	// Truncate table for asset ask order placement
	apply("TRUNCATE TABLE attachment.colored_coins_ask_order_placement");
	
	// Truncate table for asset bid order placement
	apply("TRUNCATE TABLE attachment.colored_coins_bid_order_placement");
	
	// Truncate table for ask cancel order placement
	apply("TRUNCATE TABLE attachment.colored_coins_ask_order_cancellation");
	
	// Truncate table for big cancel order placement
	apply("TRUNCATE TABLE attachment.colored_coins_bid_order_cancellation");
	
    }

    private static void delete(Connection con, Boolean commitUpdate) {
	// Delete is slower than truncate, but can be rolled back

	// Delete table for messages
	apply(con,
	      "DELETE FROM attachment.messaging_arbitrary_message",
	      false);
	
	// Delete table for aliases
	apply(con,
	      "DELETE FROM attachment.messaging_alias_assignment",
	      false);
	
	// Delete table for poll creation
	apply(con,
	      "DELETE FROM attachment.messaging_poll_creation",
	      false);
	
	// Delete table for poll vote
	apply(con,
	      "DELETE FROM attachment.messaging_vote_casting",
	      false);
	
	// Delete table for asset issuance
	apply(con,
	      "DELETE FROM attachment.colored_coins_asset_issuance",
	      false);
	
	// Delete table for asset transfer
	apply(con,
	      "DELETE FROM attachment.colored_coins_asset_transfer",
	      false);
	
	// Delete table for asset ask order placement
	apply(con,
	      "DELETE FROM attachment.colored_coins_ask_order_placement",
	      false);
	
	// Delete table for asset bid order placement
	apply(con,
	      "DELETE FROM attachment.colored_coins_bid_order_placement",
	      false);
	
	// Delete table for ask cancel order placement
	apply(con,
	      "DELETE FROM attachment.colored_coins_ask_order_cancellation",
	      false);
	
	// Delete table for big cancel order placement
	apply(con,
	      "DELETE FROM attachment.colored_coins_bid_order_cancellation",
	      false);
	
	// One commit for all if desired
	apply(con,null,commitUpdate);
	
    }

    private static void viewsFullTransaction(Connection con, Boolean commitUpdate) {
	// Views instead of duplicate columns

	// Transaction view for messages
	apply(con,
	      "CREATE VIEW attachment.trans_messaging_arbitrary_message AS " +
	      "SELECT t.*, a.* FROM " +
	      "public.transaction t join attachment.messaging_arbitrary_message a " +
	      "ON t.id=a.transaction_id",
	      false);
	
	// Transaction view for aliases
	apply(con,
	      "CREATE VIEW attachment.trans_messaging_alias_assignment AS " +
	      "SELECT t.*, a.* FROM " +
	      "public.transaction t join attachment.messaging_alias_assignment a " +
	      "ON t.id=a.transaction_id",
	      false);
	
	// Transaction view for poll creation
	apply(con,
	      "CREATE VIEW attachment.trans_messaging_poll_creation AS " +
	      "SELECT t.*, a.* FROM " +
	      "public.transaction t join attachment.messaging_poll_creation a " +
	      "ON t.id=a.transaction_id",
	      false);
	
	// Transaction view for poll vote
	apply(con,
	      "CREATE VIEW attachment.trans_messaging_vote_casting AS " +
	      "SELECT t.*, a.* FROM " +
	      "public.transaction t join attachment.messaging_vote_casting a " +
	      "ON t.id=a.transaction_id",
	      false);
	
	// Transaction view for asset issuance
	apply(con,
	      "CREATE VIEW attachment.trans_colored_coins_asset_issuance AS " +
	      "SELECT t.*, a.* FROM " +
	      "public.transaction t join attachment.colored_coins_asset_issuance a " +
	      "ON t.id=a.transaction_id",
	      false);
	
	// Transaction view for asset transfer
	apply(con,
	      "CREATE VIEW attachment.trans_colored_coins_asset_transfer AS " +
	      "SELECT t.*, a.* FROM " +
	      "public.transaction t join attachment.colored_coins_asset_transfer a " +
	      "ON t.id=a.transaction_id",
	      false);
	
	// Transaction view for asset ask order placement
	apply(con,
	      "CREATE VIEW attachment.trans_colored_coins_ask_order_placement AS " +
	      "SELECT t.*, a.* FROM " +
	      "public.transaction t join attachment.colored_coins_ask_order_placement a " +
	      "ON t.id=a.transaction_id",
	      false);
	
	// Transaction view for asset bid order placement
	apply(con,
	      "CREATE VIEW attachment.trans_colored_coins_bid_order_placement AS " +
	      "SELECT t.*, a.* FROM " +
	      "public.transaction t join attachment.colored_coins_bid_order_placement a " +
	      "ON t.id=a.transaction_id",
	      false);
	
	// Transaction view for ask cancel order placement
	apply(con,
	      "CREATE VIEW attachment.trans_colored_coins_ask_order_cancellation AS " +
	      "SELECT t.*, a.* FROM " +
	      "public.transaction t join attachment.colored_coins_ask_order_cancellation a " +
	      "ON t.id=a.transaction_id",
	      false);
	
	// Transaction view for big cancel order placement
	apply(con,
	      "CREATE VIEW attachment.trans_colored_coins_bid_order_cancellation AS " +
	      "SELECT t.*, a.* FROM " +
	      "public.transaction t join attachment.colored_coins_bid_order_cancellation a " +
	      "ON t.id=a.transaction_id",
	      false);
	
	// One commit for all if desired
	apply(con,null,commitUpdate);
	
    }

    private static void insert(Connection con, Boolean commitUpdate) {
	// Use table-specific prepared statements for speed

	try (Statement stmt = con.createStatement();
	     PreparedStatement p_messaging_arbitrary_message=
	     con.prepareStatement("INSERT INTO attachment.messaging_arbitrary_message" +
				  " (transaction_id,message) " +
				  " VALUES (?,?)");
	     PreparedStatement p_messaging_alias_assignment=
	     con.prepareStatement("INSERT INTO attachment.messaging_alias_assignment" +
				  " (transaction_id,name,uri) " +
				  " VALUES (?,?,?)");
	     PreparedStatement p_messaging_poll_creation=
	     con.prepareStatement("INSERT INTO attachment.messaging_poll_creation" +
				  " (transaction_id,name,description,options,min_number_of_options," +
				  "max_number_of_options,options_are_binary) " +
				  " VALUES (?,?,?,?,?,?,?)");
	     PreparedStatement p_messaging_vote_casting=
	     con.prepareStatement("INSERT INTO attachment.messaging_vote_casting" +
				  "(transaction_id,poll_id,vote)" +
				  " VALUES (?,?,?)");
	     PreparedStatement p_colored_coins_asset_issuance=
	     con.prepareStatement("INSERT INTO attachment.colored_coins_asset_issuance" +
				  "(transaction_id,name,description,quantity)" +
				  " VALUES (?,?,?,?)");
	     PreparedStatement p_colored_coins_asset_transfer=
	     con.prepareStatement("INSERT INTO attachment.colored_coins_asset_transfer" +
				  "(transaction_id,asset,quantity,comment)" +
				  " VALUES (?,?,?,?)");
	     PreparedStatement p_colored_coins_ask_order_placement=
	     con.prepareStatement("INSERT INTO attachment.colored_coins_ask_order_placement" +
				  "(transaction_id,asset,quantity,price)" +
				  " VALUES (?,?,?,?)");
	     PreparedStatement p_colored_coins_bid_order_placement=
	     con.prepareStatement("INSERT INTO attachment.colored_coins_bid_order_placement" +
				  "(transaction_id,asset,quantity,price)" +
				  " VALUES (?,?,?,?)");
	     PreparedStatement p_colored_coins_ask_order_cancellation=
	     con.prepareStatement("INSERT INTO attachment.colored_coins_ask_order_cancellation" +
				  "(transaction_id,order_id)" +
				  " VALUES (?,?)");
	     PreparedStatement p_colored_coins_bid_order_cancellation=
	     con.prepareStatement("INSERT INTO attachment.colored_coins_bid_order_cancellation" +
				  "(transaction_id,order_id)" +
				  " VALUES (?,?)")
	     ) {
		ResultSet rs = stmt.executeQuery("SELECT id, attachment FROM public.transaction " + 
						  "WHERE attachment IS NOT NULL");

		while (rs.next()) {
		    long id = rs.getLong("id");
		    Attachment a = (Attachment)rs.getObject("attachment");		    

		    // This if instanceof isn't very elegant, but is perfectly legal
		    // and probably just as efficient as anything else.
		    if (a instanceof Attachment.MessagingArbitraryMessage) {
			Attachment.MessagingArbitraryMessage mam = 
			    (Attachment.MessagingArbitraryMessage)a;
			p_messaging_arbitrary_message.setLong(1,id);
			p_messaging_arbitrary_message.setBytes(2,mam.getMessage());
			p_messaging_arbitrary_message.executeUpdate();			    
		    } else if (a instanceof Attachment.MessagingAliasAssignment) {
			Attachment.MessagingAliasAssignment maa = 
			    (Attachment.MessagingAliasAssignment)a;
			p_messaging_alias_assignment.setLong(1,id);
			p_messaging_alias_assignment.setString(2,maa.getAliasName());
			p_messaging_alias_assignment.setString(3,maa.getAliasURI());
			p_messaging_alias_assignment.executeUpdate();
		    } else if (a instanceof Attachment.MessagingPollCreation) {
			Attachment.MessagingPollCreation mpc = 
			    (Attachment.MessagingPollCreation)a;
			p_messaging_poll_creation.setLong(1,id);
			p_messaging_poll_creation.setString(2,mpc.getPollName());
			p_messaging_poll_creation.setString(3,mpc.getPollDescription());
			Array sqlArray = con.createArrayOf("String", mpc.getPollOptions());
			p_messaging_poll_creation.setArray(4,sqlArray);
			p_messaging_poll_creation.setByte(5,mpc.getMinNumberOfOptions());	
			p_messaging_poll_creation.setByte(6,mpc.getMaxNumberOfOptions());
			p_messaging_poll_creation.setBoolean(7,mpc.isOptionsAreBinary());
			p_messaging_poll_creation.executeUpdate();			    
		    } else if (a instanceof Attachment.MessagingVoteCasting) {
			Attachment.MessagingVoteCasting mvc = 
			    (Attachment.MessagingVoteCasting)a;
			p_messaging_vote_casting.setLong(1,id);
			p_messaging_vote_casting.setLong(2,mvc.getPollId());
			p_messaging_vote_casting.setBytes(3,mvc.getPollVote());
			p_messaging_vote_casting.executeUpdate();			    
		    } else if (a instanceof Attachment.ColoredCoinsAssetIssuance) {
			Attachment.ColoredCoinsAssetIssuance ccai = 
			    (Attachment.ColoredCoinsAssetIssuance)a;
			p_colored_coins_asset_issuance.setLong(1,id);
			p_colored_coins_asset_issuance.setString(2,ccai.getName());
			p_colored_coins_asset_issuance.setString(3,ccai.getDescription());
			p_colored_coins_asset_issuance.setInt(4,ccai.getQuantity());
			p_colored_coins_asset_issuance.executeUpdate();			    
		    } else if (a instanceof Attachment.ColoredCoinsAssetTransfer) {
			Attachment.ColoredCoinsAssetTransfer ccat = 
			    (Attachment.ColoredCoinsAssetTransfer)a;
			p_colored_coins_asset_transfer.setLong(1,id);
			p_colored_coins_asset_transfer.setLong(2,ccat.getAssetId());
			p_colored_coins_asset_transfer.setInt(3,ccat.getQuantity());
			p_colored_coins_asset_transfer.setString(4,ccat.getComment());
			p_colored_coins_asset_transfer.executeUpdate();
		    } else if (a instanceof Attachment.ColoredCoinsAskOrderPlacement) {
			Attachment.ColoredCoinsAskOrderPlacement ccaop = 
			    (Attachment.ColoredCoinsAskOrderPlacement)a;
			p_colored_coins_ask_order_placement.setLong(1,id);
			p_colored_coins_ask_order_placement.setLong(2,ccaop.getAssetId());
			p_colored_coins_ask_order_placement.setInt(3,ccaop.getQuantity());
			p_colored_coins_ask_order_placement.setLong(4,ccaop.getPrice());
			p_colored_coins_ask_order_placement.executeUpdate();			    
		    } else if (a instanceof Attachment.ColoredCoinsBidOrderPlacement) {
			Attachment.ColoredCoinsBidOrderPlacement ccbop = 
			    (Attachment.ColoredCoinsBidOrderPlacement)a;
			p_colored_coins_bid_order_placement.setLong(1,id);
			p_colored_coins_bid_order_placement.setLong(2,ccbop.getAssetId());
			p_colored_coins_bid_order_placement.setInt(3,ccbop.getQuantity());
			p_colored_coins_bid_order_placement.setLong(4,ccbop.getPrice());
			p_colored_coins_bid_order_placement.executeUpdate();			    
		    } else if (a instanceof Attachment.ColoredCoinsAskOrderCancellation) {
			Attachment.ColoredCoinsAskOrderCancellation ccaoc = 
			    (Attachment.ColoredCoinsAskOrderCancellation)a;
			p_colored_coins_ask_order_cancellation.setLong(1,id);
			p_colored_coins_ask_order_cancellation.setLong(2,ccaoc.getOrderId());
			p_colored_coins_ask_order_cancellation.executeUpdate();			    
		    } else if (a instanceof Attachment.ColoredCoinsBidOrderCancellation) {
			Attachment.ColoredCoinsBidOrderCancellation ccboc = 
			    (Attachment.ColoredCoinsBidOrderCancellation)a;
			p_colored_coins_bid_order_cancellation.setLong(1,id);
			p_colored_coins_bid_order_cancellation.setLong(2,ccboc.getOrderId());
			p_colored_coins_bid_order_cancellation.executeUpdate();			    
		    } else {
			throw new RuntimeException("Unknown attachment type");
		    }
		}
	    } catch (SQLException e) {
	    throw new RuntimeException(e.toString(), e);
	}

	// One commit for all if desired
	// Could be memory intensive
	apply(con,null,commitUpdate);
	
    }

    private static void init() {
        try (Connection con = Db.getConnection()) {
		drop(con,false); // Start from scratch
		create(con,false);
		//delete(con,false);
		insert(con,false);
		viewsFullTransaction(con,false);
		//indexes(con,false);
		//foreignKeys(con,false);

		// commit once for all		
		// Could be memory intensive
		apply(con,null,true); 
        } catch (SQLException e) {
            throw new RuntimeException("Can't get DB connection", e);
        }
    }

    public static void main(String[] args) throws Exception {
        Db.init();

	init();

	Db.shutdown();
    }

    private AttachmentSchema() {} // never

}
