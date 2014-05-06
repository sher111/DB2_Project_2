package simpledb.index.ehash;

import java.util.ArrayList;

import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

/**
 * An extensible hash implementation of the Index interface.
 * A unfixed number of buckets is allocated (currently, 4),
 * and each bucket is implemented as a file of index records.
 */
public class EHashIndex implements Index {
	public static int NUM_BUCKETS = 4; //must be 2^global_index
	private static int global_depth = 2;
	private String idxname;
	private Schema sch;
	private Transaction tx;
	private Constant searchkey = null;
	private TableScan ts = null;
	private static boolean first = true;
	private static ArrayList<Integer> indexes = new ArrayList<Integer>();

	/**
	 * Opens an extensible hash index for the specified index.
	 * @param idxname the name of the index
	 * @param sch the schema of the index records
	 * @param tx the calling transaction
	 */
	public EHashIndex(String idxname, Schema sch, Transaction tx) {
		this.idxname = idxname;
		this.sch = sch;
		this.tx = tx;
		if (first) {
			System.err.println("aa;lsdkjf;alksdfj;alskdjf;alskjdf;alskjf;laskdjf;alskdjf;alskdfj;alskdjf;alskdjf;alskdjfa;slfj;alskdfj;alskdfja;sldkfj");
			first = false;
			for (int i = 0; i < NUM_BUCKETS; i++) {
				indexes.add(i, global_depth);
			}
		}
	}

	/**
	 * Positions the index before the first index record
	 * having the specified search key.
	 * The method hashes the search key to determine the bucket,
	 * and then opens a table scan on the file
	 * corresponding to the bucket.
	 * The table scan for the previous bucket (if any) is closed.
	 * @see simpledb.index.Index#beforeFirst(simpledb.query.Constant)
	 */
	public void beforeFirst(Constant searchkey) {
		close();
		this.searchkey = searchkey;
		int bucket = getBucket(searchkey);
		//additions
		int local_index = indexes.get(bucket);
		String sigbin = toSigBinary(bucket, local_index);
		String tblname = idxname + sigbin;
		//end additions
		TableInfo ti = new TableInfo(tblname, sch);
		ts = new TableScan(ti, tx);
	}

	/**
	 *Converts a number to binary using a significance value
	 */
	public String toSigBinary(int bucket, int local_index) {

		String result = Integer.toBinaryString(bucket);

		if (result.length() >= local_index) {
			result = result.substring(0, local_index-1);
		}

		return result;

	}
	

	/*
	 *Expand the ArrayList, duplicate the values.
	 *TODO fix?
	 */
	public void increaseGlobal() {

		for (int i = 0; i < NUM_BUCKETS; i++) {
			indexes.add(i+NUM_BUCKETS, indexes.get(i));
		}

		global_depth++;
		NUM_BUCKETS = NUM_BUCKETS * 2;

	}

	/**
	 * Moves to the next record having the search key.
	 * The method loops through the table scan for the bucket,
	 * looking for a matching record, and returning false
	 * if there are no more such records.
	 * @see simpledb.index.Index#next()
	 */
	public boolean next() {
		while (ts.next())
			if (ts.getVal("dataval").equals(searchkey))
				return true;
		return false;
	}

	/**
	 * Retrieves the dataRID from the current record
	 * in the table scan for the bucket.
	 * @see simpledb.index.Index#getDataRid()
	 */
	public RID getDataRid() {
		int blknum = ts.getInt("block");
		int id = ts.getInt("id");
		return new RID(blknum, id);
	}

	/**
	 * Inserts a new record into the table scan for the bucket.
	 * @see simpledb.index.Index#insert(simpledb.query.Constant, simpledb.record.RID)
	 */
	public void insert(Constant val, RID rid) {
		if (getBucket(val) == 9) {
			System.err.println("FUCKING 9!!!!!!!!!!!!!!!!!!!!!!!!!");
			return;
		}
		System.out.println(getBucket(val));
		beforeFirst(val);
		if (ts.eInsert()) {
			ts.setInt("block", rid.blockNumber());
			ts.setInt("id", rid.id());
			ts.setVal("dataval", val);
		}
		else {
			System.err.println("\nAdding Buffer");
			
			int bucket = getBucket(val);
			System.err.println("Orig bucket:\t" + bucket);
			bucket = bucket % ((Double)Math.pow(2, indexes.get(bucket))).intValue();
			System.err.println("New bucket:\t" + bucket);
			
			indexes.set(bucket, indexes.get(bucket) + 1); // Change local depth
			
			System.err.println(toString());
			System.err.println("bucket:" + bucket);
			System.err.println("local:" + indexes.get(bucket));
			if (global_depth < indexes.get(bucket)) {
				System.err.println("Expanding global");
				increaseGlobal();
			}
			System.err.println(toString());

			int newBucket = bucket + (NUM_BUCKETS / 2);
			indexes.set(newBucket, indexes.get(bucket));
			
			int local_index = indexes.get(newBucket);
			String sigbin = toSigBinary(newBucket, local_index);
			String tblname = idxname + sigbin;
			TableInfo ti = new TableInfo(tblname, sch);
			TableScan newTable = new TableScan(ti, tx);
			
			newTable.beforeFirst();
			ts.beforeFirst();
			
			while (ts.next()) {
				Constant toCheck = ts.getVal("dataval");
				int checkBucket = getBucket(toCheck);
				if (checkBucket != bucket) {	// If it belongs in the new bucket
					// Insert toCheck into newTable
					int blknum = ts.getInt("block");
					int id = ts.getInt("id");
					newTable.insert();
					newTable.setInt("block", blknum);
					newTable.setInt("id", id);
					newTable.setVal("dataval", toCheck);
					
					// Delete toCheck from ts
					ts.delete();
				}
			}
			System.out.println("Recursing");
			insert(val, rid);
		}
	}

	/**
	 * Deletes the specified record from the table scan for
	 * the bucket.  The method starts at the beginning of the
	 * scan, and loops through the records until the
	 * specified record is found.
	 * @see simpledb.index.Index#delete(simpledb.query.Constant, simpledb.record.RID)
	 */
	public void delete(Constant val, RID rid) {
		beforeFirst(val);
		while(next())
			if (getDataRid().equals(rid)) {
				ts.delete();
				return;
			}
	}

	/**
	 * Closes the index by closing the current table scan.
	 * @see simpledb.index.Index#close()
	 */
	public void close() {
		if (ts != null)
			ts.close();
	}

	/**
	 * Returns the cost of searching an index file having the
	 * specified number of blocks.
	 * The method assumes that all buckets are about the
	 * same size, and so the cost is simply the size of
	 * the bucket.
	 * @param numblocks the number of blocks of index records
	 * @param rpb the number of records per block (not used here)
	 * @return the cost of traversing the index
	 */
	public static int searchCost(int numblocks, int rpb){
		return numblocks / EHashIndex.NUM_BUCKETS;
	}
	
	private int getBucket (Constant val) {
		return val.hashCode() % NUM_BUCKETS;
	}
	
	public String toString() {
		String toReturn = "";
		toReturn += "globalDepth:\t" + global_depth;
		toReturn += "\nnumBuckets:\t" + NUM_BUCKETS;
		toReturn += "\nindicies:\t" + this.indexes.size();
		toReturn += "\nsearchKey:\t" + this.searchkey;
		
		return toReturn;
		
	}
}
