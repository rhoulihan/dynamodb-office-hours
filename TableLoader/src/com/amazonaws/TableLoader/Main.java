package com.amazonaws.TableLoader;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.json.JSONObject;
//import org.springframework.http.HttpMethod;
//import org.springframework.http.MediaType;
//import org.springframework.web.reactive.function.BodyInserters;
//import org.springframework.web.reactive.function.client.WebClient;
//import org.springframework.web.reactive.function.client.WebClient.RequestBodyUriSpec;

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
	public static Map<String, List<Item>> sItems = new HashMap<String, List<Item>>();
	public static ThreadPoolExecutor tpe = (ThreadPoolExecutor) Executors.newFixedThreadPool(60);
	public static int count = 0;

	private static long elapsed, WCU = 0L, RCU = 0L;
	private static Map<String, Integer> counts = new HashMap<String, Integer>();
	private static TableWriteItems twi;
	private static String table = "", data = "", demo = "index-lag", leadingKey = "Item";
	private static List<String> keys = new ArrayList<String>();
	private static boolean createTable = false, optimizeKeys = false, shootout = false, loadItems = true;
	private static Random random = new Random();
	private static Calendar cal = Calendar.getInstance();
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

	// main function
	public static void main(String[] args) {
		Scanner scanner = new Scanner(System.in);
		disableWarning();

		// initialize DynamoDB connection
		init();

		// set globals
		parseArgs(args);
		shootout = demo.equals("shootout");

		// create the table and index if -c was passed
		if (createTable) {
			createTable(table);

			if (shootout) {
				createTable("Customers");
				createTable("Orders");
				createTable("OrderItems");
				createTable("Products");
				createTable("Invoices");
				createTable("Warehouses");
				createTable("Shipments");
				createTable("ShipmentItems");
			}
		} else {
			if (loadItems) {
				clearTable(table);

				if (shootout) {
					clearTable("Customers");
					clearTable("Orders");
					clearTable("OrderItems");
					clearTable("Invoices");
					clearTable("Shipments");
					clearTable("ShipmentItems");
					clearTable("Warehouses");
					clearTable("Products");
				}
			}
		}

		// set the list of keys to use if -o was passed
		if (optimizeKeys)
			setKeys();

		// run the specified demo
		switch (demo) {
		case "index-lag":
			// load the table
			loadItems("data", counts.get("items"), null);

			// scan the GSI until count matches numItems
			scanTable(tpe.getMaximumPoolSize(), true);
			break;

		case "shootout":
		case "online-shop":
			if (loadItems) {
				Map<String, String> params = new HashMap<String, String>();
				params.put("address",
						"{\"Country\":{\"S\":\"Sweden\"},\"County\":{\"S\":\"Vastra Gotaland\"},\"City\":{\"S\":\"Goteborg\"},\"Street\":{\"S\":\"MainStreet\"},\"Number\":{\"S\":\"20\"},\"ZipCode\":{\"S\":\"41111\"}}");
				loadItems("warehouse", 1, params);
				params.put("address",
						"{\"Country\":{\"S\":\"Sweden\"},\"County\":{\"S\":\"Vastra Gotaland\"},\"City\":{\"S\":\"Boras\"},\"Street\":{\"S\":\"RiverStreet\"},\"Number\":{\"S\":\"20\"},\"ZipCode\":{\"S\":\"11111\"}}");
				loadItems("warehouse", 1, params);

				params.clear();
				loadItems("product", counts.get("products"), params);
				params.clear();
				loadItems("customer", counts.get("customers"), params);
				drainQueue();
			}

			if (demo.equals("shootout")) {
				// Scan Orders table to get all orderId's
				System.out.print("Retrieving ID's for all Orders...");
				scanTable("Orders");
				System.out.println(String.format("\nRetrieved %d Order ID's.", count));

				// Prewarm thread pool
				System.out.println("Prewarming thread pool...");
				tpe.prestartAllCoreThreads();
				getAllOrdersById(false);

				// Start the test
				for (int i = 0; i < 100; i++) {
					System.out.println(String.format("\nIteration %d:", i));
					
					// Run Multi-table and record execution time
					count = 0;
					System.out.print("Running getOrderById test for Multiple Table data model...");
					elapsed = System.currentTimeMillis();
					getAllOrdersById(false);

					long multiTable = System.currentTimeMillis() - elapsed;
					System.out.println(String.format("\nRetrieved %d order objects with average latency of %dms,",
							count, multiTable / count));

					// Reset, run Single table and record time
					sItems = new HashMap<String, List<Item>>();
					count = 0;
					System.out.print("\nRunning getOrderById test for Single Table data model...");
					elapsed = System.currentTimeMillis();
					getAllOrdersById(true);

					long singleTable = System.currentTimeMillis() - elapsed;
					System.out.println(String.format("\nRetrieved %d order objects with average latency of %dms,",
							count, singleTable / count));
					
					// Report Single table efficiency as a percentage of Multi-table response time
					System.out.println(String.format("Single table efficiency: %d%s", (singleTable * 100) / (multiTable), "%"));
				}
			}
			break;
		}

		// shutdown the thread pool and exit
		System.out.println("Shutting down....");
		scanner.close();
		tpe.shutdown();
		System.out.println("Done.\n");
	}

	private static void getAllOrdersById(boolean singleTable) {
		for (List<Item> items : results.values()) {
			for (Item item : items) {
				if (singleTable) {
					// Get all items from same table
					numThreads.incrementAndGet();
					tpe.execute(new RunQuery(table, item.getString("PK")));
				} else {
					// Get items from entity specific tables
					numThreads.incrementAndGet();
					tpe.execute(new RunQuery("Orders", item.getString("PK")));
					numThreads.incrementAndGet();
					tpe.execute(new RunQuery("OrderItems", item.getString("PK")));
					numThreads.incrementAndGet();
					tpe.execute(new RunQuery("Invoices", item.getString("PK")));
					numThreads.incrementAndGet();
					tpe.execute(new RunQuery("Shipments", item.getString("PK")));
					numThreads.incrementAndGet();
					tpe.execute(new RunQuery("ShipmentItems", item.getString("PK")));
				}
			}
			count += items.size();
		}
		
		// Wait until workers are done
		waitForWorkers("");
	}

	private static void scanTable(String name) {
		for (int i = 0; i < tpe.getMaximumPoolSize(); i++) {
			tpe.execute(new RunScan(i, tpe.getMaximumPoolSize(), name, false));
		}

		waitForWorkers(".");
	}

	// blow away all the items from a previous run
	private static void clearTable(String name) {
		System.out.print(String.format("Clearing items from table [%s]...", name));
		elapsed = System.currentTimeMillis();

		scanTable(name);

		// delete all the items returned from scan
		twi = new TableWriteItems(name);
		count = 0;
		for (Integer key : results.keySet()) {
			List<Item> resultItems = results.get(key);
			for (Item item : resultItems)
				removeItem(item);

			count += resultItems.size();
		}

		removeItem(null);
		waitForWorkers(".");

		System.out.println(String.format("\nDeleted %d items in %dms.", count, System.currentTimeMillis() - elapsed));
		results.clear();
	}

	private static void removeItem(Item item) {
		if (item != null) {
			twi.addHashAndRangePrimaryKeysToDelete("PK", "SK", item.get("PK"), item.get("SK"));

			// check if we need to send a batch write
			if (twi.getPrimaryKeysToDelete().size() == 25) {
				tpe.execute(new BatchLoad(twi));
				twi = new TableWriteItems(twi.getTableName());
			}
		} else if (twi.getPrimaryKeysToDelete() != null) {
			tpe.execute(new BatchLoad(twi));
			twi = new TableWriteItems(twi.getTableName());
		}
	}

	private static int loadItems(String type, int qty, Map<String, String> params) {
		int ret = 0, count = 0;
		elapsed = System.currentTimeMillis();

		if (demo.equals("index-lag"))
			System.out.print(String.format("Loading %s items...", type));

		// load the items
		if (twi == null)
			twi = new TableWriteItems(table);
		for (count = 0; count < qty; count++) {
			String pk = String.format("%s#%d", leadingKey, count), sk;
			Item item;

			switch (type) {
			case "donor":
				saveItem(new Item().withString("PK", pk).withString("SK", "A"));
				break;

			case "data":
				pk = String.format("Shard_%d", count % counts.get("partitions"));
				// if list of optimized keys is empty then just spread items out randomly on GSI
				String gsipk = (keys.size() == 0 ? String.format("Shard_%d", count % counts.get("partitions"))
						: keys.get(count % counts.get("partitions")));
				saveItem(new Item().withString("PK", pk).withString("SK", "A").withString("GSI1PK", gsipk)
						.withString("GSI1SK", pk).withString("data", data));
				break;

			case "customer":
				pk = String.format("C#%d", counts.put("customers", counts.get("customers") + 1));
				params.put("customerId", pk);

				queueItem(new Item().withString("PK", pk).withString("SK", pk).withString("type", "customer")
						.withString("email", String.format("%s@somewhere.com", getString(10))));

				loadItems("order", random.nextInt((counts.get("orders") != null ? counts.get("orders") : 5)), params);
				break;

			case "order":
				pk = String.format("O#%d", counts.put("items", counts.get("items") + 1));
				params.put("orderId", pk);

				params.put("amount",
						Integer.toString(loadItems("orderItem",
								random.nextInt((counts.get("orderitems") != null ? counts.get("orderitems") : 3)) + 1,
								params)));

				cal.add(Calendar.DAY_OF_YEAR, random.nextInt(30) * -1);
				queueItem(new Item().withString("PK", pk).withString("SK", params.get("customerId"))
						.withString("type", "order").withString("date", sdf.format(cal.getTime()))
						.withNumber("amount", Integer.valueOf(params.get("amount"))));

				if (random.nextBoolean()) {
					loadItems("invoice", 1, params);
					loadItems("shipment", 1, params);
				}

				cal = Calendar.getInstance();
				results.put(1, null);
				break;

			case "invoice":
				pk = params.get("orderId");
				sk = String.format("I#%d", counts.put("items", counts.get("items") + 1));
				cal.add(Calendar.DAY_OF_YEAR, 1);
				item = new Item().withString("PK", pk).withString("SK", sk).withString("type", "invoice")
						.withString("GSI1PK", sk).withString("GSI1SK", sk)
						.withString("GSI2PK", params.get("customerId")).withString("GSI2SK", sdf.format(cal.getTime()))
						.withNumber("amount", Integer.valueOf(params.get("amount")))
						.withString("date", sdf.format(cal.getTime()));

//				if (random.nextBoolean()) {
//					JSONObject payment = new JSONObject(String.format(
//							"{\"Payments\":[{\"Type\": \"MasterCard\",\"Amount\":%s,\"Data\":\"Payment data here...\"}]}",
//							params.get("amount")));
//
//					item.withMap("detail", payment.toMap());
//				}

				queueItem(item);
				break;

			case "orderItem":
				Item pItem = results.get(0).get(random.nextInt(results.get(0).size()));
				pk = params.get("orderId");
				sk = String.format("%s#%d", pItem.getString("PK"), count);

				try {
					item = new Item().withString("PK", pk).withString("SK", sk)
							.withString("GSI1PK", pItem.getString("PK")).withString("type", "orderItem")
							.withString("GSI1SK", sdf.format(cal.getTime()))
							.withString("GSI2PK", params.get("customerId"))
							.withString("GSI2SK", sdf.format(cal.getTime())).withNumber("qty", random.nextInt(5))
							.withNumber("price", pItem.getNumber("price"));

					if (results.get(1) == null)
						results.put(1, new ArrayList<Item>());

					results.get(1).add(item);
					queueItem(item);

					ret += item.getNumber("qty").multiply(item.getNumber("price")).intValue();
				} catch (Exception ex) {
					System.out.println(String.format("pItem: ", pItem.toJSON()));
				}
				break;

			case "shipment":
				pk = params.get("orderId");
				sk = String.format("S#%d", counts.put("items", counts.get("items") + 1));

				params.put("shipmentId", sk);
				JSONObject shipTo = new JSONObject(
						"{\"Country\": \"Sweden\",\"County\":  \"Vastra Gotaland\",\"City\":  \"Goteborg\",\"Street\":  \"Slanbarsvagen\",\"Number\":  \"34\",\"ZipCode\": \"41787\"}");

				item = new Item().withString("PK", pk).withString("SK", sk).withString("type", "shipment")
						.withString("GSI1PK", sk).withString("GSI1SK", sk)
						.withString("GSI2PK", keys.get(random.nextInt(keys.size())))
						.withString("GSI2SK", sdf.format(cal.getTime())) // .withMap("address", shipTo.toMap())
						.withString("method", (random.nextBoolean() ? "Express" : "Standard"));

				queueItem(item);

				loadItems("shipItem", results.get(1).size(), params);
				break;

			case "shipItem":
				Item orderItem = results.get(1).get(count);
				pk = params.get("orderId");
				sk = String.format("SI#%d", counts.put("items", counts.get("items") + 1));
				item = new Item().withString("PK", pk).withString("SK", sk).withString("type", "shipItem")
						.withString("GSI1PK", params.get("shipmentId"))
						.withString("GSI1SK", orderItem.getString("GSI1PK"))
						.withNumber("qty", orderItem.getNumber("qty"));

				queueItem(item);
				break;

			case "warehouse":
				pk = String.format("W#%d", counts.put("items", counts.get("items") + 1));
				keys.add(pk);
				JSONObject shipFrom = new JSONObject(params.get("address"));

				queueItem(new Item().withString("PK", pk).withString("SK", pk).withString("type", "warehouse")
						.withMap("address", shipFrom.toMap()));
				break;

			case "product":
				pk = String.format("P#%d", counts.put("items", counts.get("items") + 1));
				sk = keys.get(random.nextInt(keys.size()));
				JSONObject product = new JSONObject(String.format(
						"{\"Name\":{\"S\":\"Product%d\"},\"Description\":{\"S\":\"An amazing product.\"}}",
						counts.get("items")));

				item = new Item().withString("PK", pk).withString("SK", sk).withString("type", "warehouseItem")
						.withString("GSI2PK", sk).withString("GSI2PK", pk).withNumber("qty", random.nextInt(100) + 100)
						.withMap("detail", product.toMap()).withNumber("price", random.nextInt(50) + 10);

				if (results.get(Integer.valueOf(0)) == null)
					results.put(0, new ArrayList<Item>());
				results.get(0).add(item);

				queueItem(item);
				break;
			}
		}

		if (demo.equals("index-lag")) {
			// run the last batchWrite
			saveItem(null);
			waitForWorkers(".");

			// log elapsed time and wait on console input
			if (!demo.equals("online-shop"))
				System.out.println(
						String.format("\nLoaded %d items in %dms.", qty, System.currentTimeMillis() - elapsed));
		}
		return ret;
	}

	private static void drainQueue() {
		elapsed = System.currentTimeMillis();
		System.out.print("Loading items...");

		twi = new TableWriteItems("data");
		for (Item item : results.get(2)) {
			saveItem(item);

			if (shootout) {
				String type = item.getString("type");
				if (!sItems.containsKey(type))
					sItems.put(type, new ArrayList<Item>());
				sItems.get(type).add(item);
			}
		}

		saveItem(null);
		waitForWorkers(".");

		System.out.println(String.format("\nLoaded %d items in %dms.", results.get(2).size(),
				System.currentTimeMillis() - elapsed));

		if (shootout) {
			System.out.print("Loading multi-table items...");
			elapsed = System.currentTimeMillis();

			for (String key : sItems.keySet()) {
				switch (key) {
				case "customer":
					twi = new TableWriteItems("Customers");
					break;

				case "warehouse":
					twi = new TableWriteItems("Warehouses");
					break;

				case "warehouseItem":
					twi = new TableWriteItems("Products");
					break;

				case "order":
					twi = new TableWriteItems("Orders");
					break;

				case "invoice":
					twi = new TableWriteItems("Invoices");
					break;

				case "orderItem":
					twi = new TableWriteItems("OrderItems");
					break;

				case "shipment":
					twi = new TableWriteItems("Shipments");
					break;

				case "shipItem":
					twi = new TableWriteItems("ShipmentItems");
					break;
				}

				for (Item item : sItems.get(key))
					saveItem(item);
				saveItem(null);
			}

			waitForWorkers(".");

			System.out.println(String.format("\nLoaded %d items in %dms.", results.get(2).size(),
					System.currentTimeMillis() - elapsed));
		}

		results.put(2, null);
	}

	private static void queueItem(Item item) {
		if (results.get(2) == null)
			results.put(2, new ArrayList<Item>());

		results.get(2).add(item);
	}

	private static String getString(int length) {
		String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
		StringBuilder string = new StringBuilder();

		while (string.length() < length)
			string.append(chars.charAt(random.nextInt(chars.length())));

		return string.toString();
	}

	private static void saveItem(Item item) {
		// add the item to the batch
		if (item != null) {
			twi.addItemToPut(item);

			// if the container has 25 items run the batchWrite on a new thread
			if (twi.getItemsToPut().size() == 25) {
				tpe.execute(new BatchLoad(twi));
				twi = new TableWriteItems(twi.getTableName());
			}
		} else if (twi.getItemsToPut() != null) {
			tpe.execute(new BatchLoad(twi));
			twi = new TableWriteItems(twi.getTableName());
		}
	}

	private static void waitForWorkers(String printChar) {
		// sleep until all updates are done
		while (numThreads.get() > 0)
			try {
				System.out.print(printChar);
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

		// scan until the total items read matches number of items written
		while (count < counts.get("items")) {
			response = System.currentTimeMillis();
			results = new HashMap<Integer, List<Item>>();
			for (int i = 0; i < numSegments; i++) {
				tpe.execute(new RunScan(i, numSegments, table, indexScan));
			}

			waitForWorkers(".");
			count = 0;
			for (Integer key : results.keySet())
				count += results.get(key).size();

			response = System.currentTimeMillis() - response;

			// if this is not an index scan then we are generating balanced keys so bail out
			if (indexScan)
				System.out.print(String.format("\n%d of %d items replicated...", count, counts.get("items")));
			else
				break;
		}

		System.out.println(String.format("\nScan complete%s %dms.", (indexScan ? ". Replication lag " : "d in"),
				System.currentTimeMillis() - elapsed - response));
	}

	private static void setKeys() {
		System.out
				.println(String.format("Calculating %d well distributed partition keys...", counts.get("partitions")));
		long time = System.currentTimeMillis();

		// load 10K donor items to fill the keyspace
		loadItems("donor", 10000, null);

		// scan the table matching partitions to segments
		scanTable(counts.get("partitions"), false);

		// get the middle result from each scan segment
		for (List<Item> list : results.values())
			keys.add(list.get(list.size() / 2).getString("PK"));

		System.out.println(
				String.format("Distributed key generation completed in %dms.", System.currentTimeMillis() - time));

		// initialize the results container and the table
		results.clear();
		clearTable(table);
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
			case "-n":
				counts.put("customers", Integer.valueOf(argVals.get(key)));
				break;

			case "-m":
				counts.put("orders", Integer.valueOf(argVals.get(key)));
				break;

			case "-i":
				counts.put("orderItems", Integer.valueOf(argVals.get(key)));
				counts.put("items", Integer.valueOf(argVals.get(key)));
				break;

			case "-s":
				for (int i = 0; i < Integer.valueOf(argVals.get(key)); i++)
					data = String.format("%s%s", data, "X");
				break;

			case "-p":
				counts.put("partitions", Integer.valueOf(argVals.get(key)));
				counts.put("products", Integer.valueOf(argVals.get(key)));
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
				leadingKey = (argVals.get(key) != null ? argVals.get(key) : leadingKey);
				break;

			case "-d":
				demo = argVals.get(key);
				break;

			case "-l":
				loadItems = false;
				break;

			default:
				usage(String.format("ERROR: Unknown argument [%s].", key));
				break;
			}
		}

		switch (demo) {
		case "index-lag":
			if (counts.get("items") == null || counts.get("partitions") == null || table.equals("")
					|| (createTable && (WCU == 0 || RCU == 0)))
				usage(String.format("Missing required option [%s]",
						(counts.get("items") == null ? "-i"
								: (counts.get("partitions") == null ? "-p"
										: (table.equals("") ? "-t" : (WCU == 0 ? "-w" : "-r"))))));
			break;

		case "online-shop":
			if (table.equals("") || (createTable && (WCU == 0 || RCU == 0)))
				usage(String.format("Missing required option [%s]",
						(table.equals("") ? "-t" : (WCU == 0 ? "-w" : "-r"))));

			if (counts.get("products") == null)
				counts.put("products", 50);

			if (counts.get("customers") == null)
				counts.put("customers", 10);
			break;
		}
	}

	private static void usage(String message) {
		System.err.println(message);
		System.out.println("Usage: java -jar TableLoader.jar [options]");
		System.out.println("\nFor all demos:");
		System.out.println("-t  <string>\t\tTable name [REQUIRED]");
		System.out.println("-c  \t\t\tCreate the table");
		System.out.println("-w  <number>\t\tTable/index WCU");
		System.out.println("-r  <number>\t\tTable/index RCU");
		System.out.println("-d  <string>\t\tName of demo to run [index-lag, online-shop]");

		System.out.println("\nFor 'index-lag' demo:");
		System.out.println("-i  <number>\t\tNumber of items [REQUIRED]");
		System.out.println("-p  <number>\t\tNumber of index partitions [REQUIRED]");
		System.out.println("-s  <number>\t\tSize of items in bytes");
		System.out.println("-o  \t\t\tOptimize partition keys");

		System.out.println("\nFor 'online-shop' or 'shootout' demo:");
		System.out.println("-n  <number>\t\tNumber of customers");
		System.out.println("-m  <number>\t\tMaximum number of orders per customer");
		System.out.println("-i  <number>\t\tMaximum number of items per order");
		System.out.println("-p  <number>\t\tNumber of products");
		System.out.println("-l  \t\t\tSkip table loading");
		System.exit(1);
	}

	private static void createTable(String name) {
		try {
			elapsed = System.currentTimeMillis();
			System.out.println(String.format("Creating table '%s' at %d/%d RCU/WCU...", name, RCU, WCU));
			db.createTable(name,
					Arrays.asList(new KeySchemaElement("PK", KeyType.HASH), new KeySchemaElement("SK", KeyType.RANGE)),
					Arrays.asList(new AttributeDefinition("PK", ScalarAttributeType.S),
							new AttributeDefinition("SK", ScalarAttributeType.S)),
					new ProvisionedThroughput(RCU, WCU)).waitForActive();

			System.out.println(String.format("Table created in %dms", System.currentTimeMillis() - elapsed));

			switch (demo) {
			case "index-lag":
				createIndex(name, "GSI1");
				break;

			case "online-shop":
				createIndex(name, "GSI1");
				Thread.sleep(2000);
				createIndex(name, "GSI2");
				break;

			case "shootout":

				break;
			}
		} catch (InterruptedException ex) {
			System.err.println(ex.getMessage());
			System.exit(1);
		}
	}

	private static void createIndex(String root, String name) {
		try {
			ArrayList<AttributeDefinition> attrDefs = new ArrayList<AttributeDefinition>();
			attrDefs.add(
					new AttributeDefinition().withAttributeName(String.format("%sPK", name)).withAttributeType("S"));
			attrDefs.add(
					new AttributeDefinition().withAttributeName(String.format("%sSK", name)).withAttributeType("S"));

			GlobalSecondaryIndexUpdate update = new GlobalSecondaryIndexUpdate()
					.withCreate(new CreateGlobalSecondaryIndexAction().withIndexName(name)
							.withProvisionedThroughput(new ProvisionedThroughput(RCU, WCU))
							.withKeySchema(
									new KeySchemaElement().withAttributeName(String.format("%sPK", name))
											.withKeyType(KeyType.HASH),
									new KeySchemaElement().withAttributeName(String.format("%sSK", name))
											.withKeyType(KeyType.RANGE))
							.withProjection(new Projection().withProjectionType("ALL")));

			UpdateTableSpec uts = new UpdateTableSpec().withAttributeDefinitions(attrDefs)
					.withGlobalSecondaryIndexUpdates(update);

			elapsed = System.currentTimeMillis();
			System.out.println(String.format("Creating %s at %d/%d RCU/WCU...", name, RCU, WCU));

			db.getTable(root).updateTable(uts);
			db.getTable(root).getIndex(name).waitForActive();

			System.out.println(String.format("Index created in %dms", System.currentTimeMillis() - elapsed));
		} catch (InterruptedException ex) {
			System.err.println(ex.getMessage());
			System.exit(1);
		}
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
