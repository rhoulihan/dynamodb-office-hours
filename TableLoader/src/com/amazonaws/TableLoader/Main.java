package com.amazonaws.TableLoader;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateTableSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateGlobalSecondaryIndexAction;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexUpdate;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

public class Main {
	// misc globals
	public static DynamoDB db;
	public static volatile AtomicInteger numThreads = new AtomicInteger(0);
	public static volatile Object sync = new Object();
	public static volatile Map<Integer, List<Item>> results = new HashMap<Integer, List<Item>>();
	public static ThreadPoolExecutor tpe = (ThreadPoolExecutor) Executors.newFixedThreadPool(60);

	private static long elapsed, WCU = 0L, RCU = 0L;
	private static int count = 0, numItems = 0, numPartitions = 0;
	private static TableWriteItems twi;
	private static String table = "";
	private static String data = "";
	private static List<String> keys = new ArrayList<String>();
	private static boolean createTable = false, optimizeKeys = false;

	// main function
	public static void main(String[] args) {
		Scanner scanner = new Scanner(System.in);

		disableWarning();
		init();
		parseArgs(args);

		if (createTable) {
			if (WCU == 0L || RCU == 0L)
				usage(String.format("ERROR: Unable to create table with %d/%d RCU/WCU", RCU, WCU));

			createTable();
			createIndex("GSI1");
		} else {
			clearTable();

			if (optimizeKeys) {
				setKeys();
			}
		}

		loadItems("data", numItems);
		scanTable(tpe.getMaximumPoolSize(), true);

		// shutdown the thread pool and exit
		System.out.println("Shutting down....");
		scanner.close();
		tpe.shutdown();
		System.out.println("Done.\n");
	}

	private static void init() {
		// configure the client
		ClientConfiguration config = new ClientConfiguration().withConnectionTimeout(500)
				.withClientExecutionTimeout(20000).withRequestTimeout(1000).withSocketTimeout(1000)
				.withRetryPolicy(PredefinedRetryPolicies.getDynamoDBDefaultRetryPolicyWithCustomMaxRetries(20));

		db = new DynamoDB(AmazonDynamoDBClientBuilder.standard().withClientConfiguration(config)
				.withCredentials(new ProfileCredentialsProvider("default")).build());
	}

	private static void parseArgs(String[] args) {
		String last = "";
		Map<String, String> argVals = new HashMap<String, String>();
		for (String arg : args) {
			if (arg.startsWith("-")) {
				if (argVals.putIfAbsent(arg, "") != null)
					usage(String.format("ERROR: Duplicate argument [%s].", arg));

				last = arg;
			} else {
				if (last.equals(""))
					usage(String.format("ERROR: Unable to associate argument value [%s]", arg));
				else {
					argVals.put(last, arg);
					last = "";
				}
			}
		}

		for (String key : argVals.keySet()) {
			switch (key) {
			case "-i":
				numItems = Integer.valueOf(argVals.get(key));
				break;

			case "-s":
				for (int i = 0; i < Integer.valueOf(argVals.get(key)); i++)
					data = String.format("%s%s", data, "X");
				break;

			case "-p":
				numPartitions = Integer.valueOf(argVals.get(key));
				break;

			case "-t":
				table = argVals.get(key);
				break;

			case "-w":
				WCU = Long.valueOf(argVals.get(key));
				break;

			case "-r":
				RCU = Long.valueOf(argVals.get(key));
				break;

			case "-c":
				createTable = true;
				break;

			case "-o":
				optimizeKeys = true;
				break;

			default:
				usage(String.format("ERROR: Unknown argument [%s].", key));
				break;
			}
		}

		if (numItems == 0 || numPartitions == 0 || table.equals("") || (createTable && (WCU == 0 || RCU == 0)))
			usage(String.format("Missing required option [%s]", (numItems == 0 ? "-i"
					: (numPartitions == 0 ? "-p" : (table.equals("") ? "-t" : (WCU == 0 ? "-w" : "-r"))))));
	}

	private static void usage(String message) {
		System.err.println(message);
		System.out.println("Usage: java -jar TableLoader.jar [options]");
		System.out.println("-i  <number>\t\tNumber of items [REQUIRED]");
		System.out.println("-p  <number>\t\tNumber of index partitions [REQUIRED]");
		System.out.println("-t  <string>\t\tTable name [REQUIRED]");
		System.out.println("-s  <number>\t\tSize of items in bytes");
		System.out.println("-w  <number>\t\tTable WCU");
		System.out.println("-r  <number>\t\tTable RCU");
		System.out.println("-c  \t\t\tCreate the table");
		System.out.println("-o  \t\t\tOptimize partition keys");
		System.exit(1);
	}

	private static void createTable() {
		try {
			elapsed = System.currentTimeMillis();
			System.out.println(String.format("Creating table '%s' at %d/%d RCU/WCU...", table, RCU, WCU));
			db.createTable(table,
					Arrays.asList(new KeySchemaElement("PK", KeyType.HASH), new KeySchemaElement("SK", KeyType.RANGE)),
					Arrays.asList(new AttributeDefinition("PK", ScalarAttributeType.S),
							new AttributeDefinition("SK", ScalarAttributeType.S)),
					new ProvisionedThroughput(RCU, WCU)).waitForActive();

			System.out.println(String.format("Table created in %dms", System.currentTimeMillis() - elapsed));
		} catch (InterruptedException ex) {
			System.err.println(ex.getMessage());
			System.exit(1);
		}
	}

	private static void createIndex(String name) {
		try {
			ArrayList<AttributeDefinition> attrDefs = new ArrayList<AttributeDefinition>();
			attrDefs.add(new AttributeDefinition().withAttributeName("GSIPK").withAttributeType("S"));
			attrDefs.add(new AttributeDefinition().withAttributeName("GSISK").withAttributeType("S"));

			GlobalSecondaryIndexUpdate update = new GlobalSecondaryIndexUpdate()
					.withCreate(new CreateGlobalSecondaryIndexAction().withIndexName(name)
							.withProvisionedThroughput(new ProvisionedThroughput(RCU, WCU))
							.withKeySchema(new KeySchemaElement().withAttributeName("GSIPK").withKeyType(KeyType.HASH),
									new KeySchemaElement().withAttributeName("GSISK").withKeyType(KeyType.RANGE))
							.withProjection(new Projection().withProjectionType("ALL")));

			UpdateTableSpec uts = new UpdateTableSpec().withAttributeDefinitions(attrDefs)
					.withGlobalSecondaryIndexUpdates(update);

			elapsed = System.currentTimeMillis();
			System.out.println(String.format("Creating %s at %d/%d RCU/WCU...", name, RCU, WCU));

			db.getTable(table).updateTable(uts);
			db.getTable(table).waitForActive();

			System.out.println(String.format("Index created in %dms", System.currentTimeMillis() - elapsed));
		} catch (InterruptedException ex) {
			System.err.println(ex.getMessage());
			System.exit(1);
		}
	}

	// blow away all the items from a previous run
	private static void clearTable() {
		System.out.print("Clearing items from table...");
		elapsed = System.currentTimeMillis();

		for (int i = 0; i < tpe.getMaximumPoolSize(); i++) {
			tpe.execute(new RunScan(i, tpe.getMaximumPoolSize(), table, false));
		}

		waitForWorkers();

		twi = new TableWriteItems(table);
		count = 0;
		for (Integer key : results.keySet()) {
			List<Item> resultItems = results.get(key);
			for (Item item : resultItems)
				removeItem(item);

			count += resultItems.size();
		}

		removeItem(null);
		waitForWorkers();

		System.out.println(String.format("\nDeleted %d items in %dms.", count, System.currentTimeMillis() - elapsed));
	}

	private static void removeItem(Item item) {
		if (item != null) {
			twi.addHashAndRangePrimaryKeysToDelete("PK", "SK", item.get("PK"), item.get("SK"));

			// check if we need to send a batch write
			if (twi.getPrimaryKeysToDelete().size() == 25) {
				tpe.execute(new BatchLoad(twi));
				twi = new TableWriteItems(table);
			}
		} else if (twi.getPrimaryKeysToDelete() != null) {
			tpe.execute(new BatchLoad(twi));
			twi = new TableWriteItems(table);
		}
	}

	private static boolean loadItems(String type, int qty) {
		// create some customer items
		elapsed = System.currentTimeMillis();
		System.out.print(String.format("Loading %s items...", type));

		twi = new TableWriteItems(table);
		for (count = 0; count < qty; count++) {
			String pk = String.format("Item_%d", count);
			String gsipk = (keys.size() == 0 ? String.format("Shard_%d", count % numPartitions)
					: keys.get(count % numPartitions));

			if (type.equals("donor")) {
				pk = String.format("Shard_%d", count);
				saveItem(new Item().withString("PK", pk).withString("SK", "A"));
			} else
				saveItem(new Item().withString("PK", pk).withString("SK", "A").withString("GSIPK", gsipk)
						.withString("GSISK", pk).withString("data", data));
		}

		// run the last batchWrite
		saveItem(null);
		waitForWorkers();

		// log elapsed time and wait on console input
		System.out.println(String.format("\nLoaded %d items in %dms.", qty, System.currentTimeMillis() - elapsed));
		return true;
	}

	private static void saveItem(Item item) {
		// add the item to the batch
		if (item != null) {
			twi.addItemToPut(item);

			// if the container has 25 items run the batchWrite on a new thread
			if (twi.getItemsToPut().size() == 25) {
				tpe.execute(new BatchLoad(twi));
				twi = new TableWriteItems(table);
			}
		} else {
			if (twi.getItemsToPut() != null)
				tpe.execute(new BatchLoad(twi));
			twi = null;
		}
	}

	private static void waitForWorkers() {
		// sleep until all updates are done
		while (numThreads.get() > 0)
			try {
				System.out.print(".");
				Thread.sleep(100);
			} catch (InterruptedException e) {
				System.err.println(e.getMessage());
				System.exit(1);
			}
	}

	private static void scanTable(int numSegments, boolean indexScan) {
		System.out.print(String.format("Scanning %s...", (indexScan ? "GSI1" : "table")));
		elapsed = System.currentTimeMillis();
		long response = 0L;

		count = 0;
		while (count < numItems) {
			response = System.currentTimeMillis();
			results = new HashMap<Integer, List<Item>>();
			for (int i = 0; i < numSegments; i++) {
				tpe.execute(new RunScan(i, numSegments, table, indexScan));
			}

			waitForWorkers();
			count = 0;
			for (Integer key : results.keySet())
				count += results.get(key).size();

			response = System.currentTimeMillis() - response;

			if (indexScan)
				System.out.print(String.format("\n%d of %d items replicated...", count, numItems));
			else
				break;
		}

		System.out.println(String.format("\nScan complete%s %dms.", (indexScan ? ". Replication lag " : "d in"),
				System.currentTimeMillis() - elapsed - response));
	}

	private static void setKeys() {
		System.out.println(String.format("Calculating %d well distributed partition keys...", numPartitions));
		long time = System.currentTimeMillis();

		loadItems("donor", 10000);
		scanTable(numPartitions, false);

		for (List<Item> list : results.values())
			keys.add(list.get(list.size() / 2).getString("PK"));

		System.out.println(
				String.format("Distributed key generation completed in %dms.", System.currentTimeMillis() - time));

		results.clear();
		clearTable();
	}

	private static void disableWarning() {
		try {
			Field theUnsafe = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			theUnsafe.setAccessible(true);
			sun.misc.Unsafe u = (sun.misc.Unsafe) theUnsafe.get(null);

			Class<?> cls = Class.forName("jdk.internal.module.IllegalAccessLogger");
			Field logger = cls.getDeclaredField("logger");
			u.putObjectVolatile(cls, u.staticFieldOffset(logger), null);
		} catch (Exception e) {
			// ignore
		}
	}
}
