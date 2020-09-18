package com.amazonaws.TableLoader;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.Page;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;

/**
 * reads all items from a given logical partition on a DynamoDB table
 * @author rickhou
 *
 */
public class RunScan implements Runnable {
	private int shard, segments;
	private String tableName;
	private boolean indexScan;
	List<Item> items = new ArrayList<Item>();

	public RunScan(int shard, int segments, String tableName, boolean  indexScan) {
		this.shard = shard;
		this.tableName = tableName;
		this.indexScan = indexScan;
		this.segments = segments;
		
		synchronized (Main.sync) {
			Main.numThreads.incrementAndGet();
		}
	}

	/**
	 * the runnable process that executes the read
	 */
	@Override
	public void run() {
		ScanSpec spec = new ScanSpec().withSegment(shard).withTotalSegments(segments);
		ItemCollection<ScanOutcome> results = null;
		
		if (indexScan)
			results = Main.db.getTable(tableName).getIndex("GSI1").scan(spec);
		else
			results = Main.db.getTable(tableName).scan(spec);

		for (Page<Item, ScanOutcome> page : results.pages()) {
			Iterator<Item> it = page.iterator();
			
			while (it.hasNext()) {
				items.add(it.next());
			}
		}

		synchronized (Main.sync) {
			// put these results in the result map
			Main.results.put(shard, items);
			Main.numThreads.decrementAndGet();
			Main.count += items.size();
		}
	}
}
