package com.amazonaws.TableLoader;

import java.util.ArrayList;
import java.util.Iterator;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.Page;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;

/**
 * reads all items from a given logical partition on a DynamoDB table
 * @author rickhou
 *
 */
public class RunQuery implements Runnable {
	private String pKey, table;

	public RunQuery(String table, String pKey) {
		this.table = table;
		this.pKey = pKey;
	}

	/**
	 * the runnable process that executes the read
	 */
	@Override
	public void run() {
		QuerySpec spec = new QuerySpec().withKeyConditionExpression("PK = :pKey")
				.withValueMap(new ValueMap().withString(":pKey", pKey));

		ItemCollection<QueryOutcome> results = Main.db.getTable(table).query(spec);

		ArrayList<Item> items = new ArrayList<Item>();
		for (Page<Item, QueryOutcome> page : results.pages()) {
			Iterator<Item> it = page.iterator();
			while (it.hasNext()) {
				items.add(it.next());
			}
		}

		synchronized (Main.sync) {
			// put these results in the result map
			if (Main.sItems.containsKey(pKey))
				Main.sItems.get(pKey).addAll(items);
			else
				Main.sItems.put(pKey, items);
			
			Main.numThreads.decrementAndGet();
		}
	}
}
