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
    // These are stolen from TransactionType, which is bad
    // There is undoubtedly a better, safer way to check for transaction type,
    // but I don't have the java knowledge to do this.
    // Checking for instance type on result of getTransactionType?
    private static final byte TYPE_PAYMENT = 0;
    private static final byte TYPE_MESSAGING = 1;
    private static final byte TYPE_COLORED_COINS = 2;
    private static final byte SUBTYPE_PAYMENT_ORDINARY_PAYMENT = 0;
    private static final byte SUBTYPE_MESSAGING_ARBITRARY_MESSAGE = 0;
    private static final byte SUBTYPE_MESSAGING_ALIAS_ASSIGNMENT = 1;
    private static final byte SUBTYPE_MESSAGING_POLL_CREATION = 2;
    private static final byte SUBTYPE_MESSAGING_VOTE_CASTING = 3;
    private static final byte SUBTYPE_COLORED_COINS_ASSET_ISSUANCE = 0;
    private static final byte SUBTYPE_COLORED_COINS_ASSET_TRANSFER = 1;
    private static final byte SUBTYPE_COLORED_COINS_ASK_ORDER_PLACEMENT = 2;
    private static final byte SUBTYPE_COLORED_COINS_BID_ORDER_PLACEMENT = 3;
    private static final byte SUBTYPE_COLORED_COINS_ASK_ORDER_CANCELLATION = 4;
    private static final byte SUBTYPE_COLORED_COINS_BID_ORDER_CANCELLATION = 5;

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
	      ", id BIGINT NOT NULL" +
	      ", vote VARBINARY" +
	      ", FOREIGN KEY (transaction_id) REFERENCES public.transaction(id)" + 
	      " ON DELETE CASCADE ON UPDATE CASCADE" +
	      ", FOREIGN KEY (id) REFERENCES attachment.messaging_poll_creation(transaction_id)" + 
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
	      // Not sure if this key could point to attachment.colored_coins_asset_issuance(transaction_id)
	      ", FOREIGN KEY (asset) REFERENCES public.transaction(id)" + 
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
	      // Not sure if this key could point to attachment.colored_coins_asset_issuance(transaction_id)
	      ", FOREIGN KEY (asset) REFERENCES public.transaction(id)" + 
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
	      // Not sure if this key could point to attachment.colored_coins_asset_issuance(transaction_id)
	      ", FOREIGN KEY (asset) REFERENCES public.transaction(id)" + 
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
				  " (transaction_id,name,description,options,min_number_of_options,max_number_of_options,options_are_binary) " +
				  " VALUES (?,?,?,?,?,?,?)");
	     PreparedStatement p_messaging_vote_casting=
	     con.prepareStatement("INSERT INTO attachment.messaging_vote_casting" +
				  "(transaction_id,id,vote)" +
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
		ResultSet rs = stmt.executeQuery("SELECT id, type, subtype, attachment FROM public.transaction " + 
						  "WHERE attachment IS NOT NULL");

		while (rs.next()) {
		    long id = rs.getLong("id");
		    byte type = rs.getByte("type");
		    byte subtype = rs.getByte("subtype");

		    switch (type) {
		    case TYPE_MESSAGING:
			switch (subtype) {
			case SUBTYPE_MESSAGING_ARBITRARY_MESSAGE:
			    Attachment.MessagingArbitraryMessage mam = 
				(Attachment.MessagingArbitraryMessage)rs.getObject("attachment");
			    p_messaging_arbitrary_message.setLong(1,id);
			    p_messaging_arbitrary_message.setBytes(2,mam.getMessage());
			    p_messaging_arbitrary_message.executeUpdate();			    
			    break;
			case SUBTYPE_MESSAGING_ALIAS_ASSIGNMENT:
			    Attachment.MessagingAliasAssignment maa = 
				(Attachment.MessagingAliasAssignment)rs.getObject("attachment");
			    p_messaging_alias_assignment.setLong(1,id);
			    p_messaging_alias_assignment.setString(2,maa.getAliasName());
			    p_messaging_alias_assignment.setString(3,maa.getAliasURI());
			    p_messaging_alias_assignment.executeUpdate();			    
			    break;
			case SUBTYPE_MESSAGING_POLL_CREATION:
			    Attachment.MessagingPollCreation mpc = 
				(Attachment.MessagingPollCreation)rs.getObject("attachment");
			    p_messaging_poll_creation.setLong(1,id);
			    p_messaging_poll_creation.setString(2,mpc.getPollName());
			    p_messaging_poll_creation.setString(3,mpc.getPollDescription());
			    Array sqlArray = con.createArrayOf("String", mpc.getPollOptions());
			    p_messaging_poll_creation.setArray(4,sqlArray);
			    p_messaging_poll_creation.setByte(5,mpc.getMinNumberOfOptions());	
			    p_messaging_poll_creation.setByte(6,mpc.getMaxNumberOfOptions());
			    p_messaging_poll_creation.setBoolean(7,mpc.isOptionsAreBinary());
			    p_messaging_poll_creation.executeUpdate();			    
			    break;
			case SUBTYPE_MESSAGING_VOTE_CASTING:
			    Attachment.MessagingVoteCasting mvc = 
				(Attachment.MessagingVoteCasting)rs.getObject("attachment");
			    p_messaging_vote_casting.setLong(1,id);
			    p_messaging_vote_casting.setLong(2,mvc.getPollId());
			    p_messaging_vote_casting.setBytes(3,mvc.getPollVote());
			    p_messaging_vote_casting.executeUpdate();			    
			    break;
			default:
			    throw new RuntimeException("Unknown transaction subtype");
			}
			break;
		    case TYPE_COLORED_COINS:
			switch (subtype) {
			case SUBTYPE_COLORED_COINS_ASSET_ISSUANCE:
			    Attachment.ColoredCoinsAssetIssuance ccai = 
				(Attachment.ColoredCoinsAssetIssuance)rs.getObject("attachment");
			    p_colored_coins_asset_issuance.setLong(1,id);
			    p_colored_coins_asset_issuance.setString(2,ccai.getName());
			    p_colored_coins_asset_issuance.setString(3,ccai.getDescription());
			    p_colored_coins_asset_issuance.setInt(4,ccai.getQuantity());
			    p_colored_coins_asset_issuance.executeUpdate();			    
			    break;
			case SUBTYPE_COLORED_COINS_ASSET_TRANSFER:
			    Attachment.ColoredCoinsAssetTransfer ccat = 
				(Attachment.ColoredCoinsAssetTransfer)rs.getObject("attachment");
			    p_colored_coins_asset_transfer.setLong(1,id);
			    p_colored_coins_asset_transfer.setLong(2,ccat.getAssetId());
			    p_colored_coins_asset_transfer.setInt(3,ccat.getQuantity());
			    p_colored_coins_asset_transfer.setString(4,ccat.getComment());
			    p_colored_coins_asset_transfer.executeUpdate();			    
			    break;
			case SUBTYPE_COLORED_COINS_ASK_ORDER_PLACEMENT:
			    Attachment.ColoredCoinsAskOrderPlacement ccaop = 
				(Attachment.ColoredCoinsAskOrderPlacement)rs.getObject("attachment");
			    p_colored_coins_ask_order_placement.setLong(1,id);
			    p_colored_coins_ask_order_placement.setLong(2,ccaop.getAssetId());
			    p_colored_coins_ask_order_placement.setInt(3,ccaop.getQuantity());
			    p_colored_coins_ask_order_placement.setLong(4,ccaop.getPrice());
			    p_colored_coins_ask_order_placement.executeUpdate();			    
			    break;
			case SUBTYPE_COLORED_COINS_BID_ORDER_PLACEMENT:
			    Attachment.ColoredCoinsBidOrderPlacement ccbop = 
				(Attachment.ColoredCoinsBidOrderPlacement)rs.getObject("attachment");
			    p_colored_coins_bid_order_placement.setLong(1,id);
			    p_colored_coins_bid_order_placement.setLong(2,ccbop.getAssetId());
			    p_colored_coins_bid_order_placement.setInt(3,ccbop.getQuantity());
			    p_colored_coins_bid_order_placement.setLong(4,ccbop.getPrice());
			    p_colored_coins_bid_order_placement.executeUpdate();			    
			    break;
			case SUBTYPE_COLORED_COINS_ASK_ORDER_CANCELLATION:
			    Attachment.ColoredCoinsAskOrderCancellation ccaoc = 
				(Attachment.ColoredCoinsAskOrderCancellation)rs.getObject("attachment");
			    p_colored_coins_ask_order_cancellation.setLong(1,id);
			    p_colored_coins_ask_order_cancellation.setLong(2,ccaoc.getOrderId());
			    p_colored_coins_ask_order_cancellation.executeUpdate();			    
			    break;
			case SUBTYPE_COLORED_COINS_BID_ORDER_CANCELLATION:
			    Attachment.ColoredCoinsBidOrderCancellation ccboc = 
				(Attachment.ColoredCoinsBidOrderCancellation)rs.getObject("attachment");
			    p_colored_coins_bid_order_cancellation.setLong(1,id);
			    p_colored_coins_bid_order_cancellation.setLong(2,ccboc.getOrderId());
			    p_colored_coins_bid_order_cancellation.executeUpdate();			    
			    break;
			default:
			    throw new RuntimeException("Unknown transaction subtype");
			}
			break;
		    default:
			throw new RuntimeException("Unknown transaction type");
		    }
		}
	    } catch (SQLException e) {
	    throw new RuntimeException(e.toString(), e);
	}

	// One commit for all if desired
	// Could be memory intensive
	apply(con,null,commitUpdate);
	
    }

    public static void main(String[] args) throws Exception {
        Db.init();

        try (Connection con = Db.getConnection()) {
		drop(con,false); // Start from scratch
		create(con,false);
		//delete(con,false);
		insert(con,false);
		//index(con,false);
		//foreignKeys(con,false);

		// commit once for all		
		// Could be memory intensive
		apply(con,null,true); 
        } catch (SQLException e) {
            throw new RuntimeException("Can't get DB connection", e);
        }

	Db.shutdown();
    }

    private AttachmentSchema() {} // never

}
