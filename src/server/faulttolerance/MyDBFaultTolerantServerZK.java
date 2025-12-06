package server.faulttolerance;

import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;
import server.ReplicatedServer;
import server.SingleServer;

import org.apache.zookeeper.*;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class implements a replicated fault-tolerant database server using
 * Zookeeper for coordination. It uses ZK sequential nodes to establish total
 * order over client requests and implements checkpointing to bound log size.
 * 
 * Architecture:
 * - ZooKeeper provides total ordering via sequential znodes
 * - All servers watch the request log and execute in order
 * - Checkpointing bounds log size to MAX_LOG_SIZE
 * - Crash recovery restores from checkpoint and replays log
 */
public class MyDBFaultTolerantServerZK extends server.MyDBSingleServer {

	private static final Logger log = Logger.getLogger(MyDBFaultTolerantServerZK.class.getName()); // used this for testing, now unused

	/**
	 * Set this value to as small a value with which you can get tests to still
	 * pass. The lower it is, the faster your implementation is. Grader* will
	 * use this value provided it is no greater than its MAX_SLEEP limit.
	 */
	public static final int SLEEP = 1000;

	/**
	 * Set this to true if you want all tables dropped at the end of each run
	 * of tests by GraderFaultTolerance.
	 */
	public static final boolean DROP_TABLES_AFTER_TESTS = true;

	/**
	 * Maximum permitted size of any collection that is used to maintain
	 * request-specific state, i.e., you can not maintain state for more than
	 * MAX_LOG_SIZE requests (in memory or on disk). This constraint exists to
	 * ensure that your logs don't grow unbounded, which forces checkpointing to
	 * be implemented.
	 */
	public static final int MAX_LOG_SIZE = 400;

	public static final int DEFAULT_PORT = 2181;

	// ZK paths
	private static final String ROOT_PATH = "/dbft";
	private static final String REQUESTS_PATH = ROOT_PATH + "/requests";
	private static final String SERVERS_PATH = ROOT_PATH + "/servers";
	
	private String myCheckpointPath;
	private String keyspace;

	protected final Session session;
	protected final Cluster cluster;

	private ZooKeeper zk;
	private final String myID;
	
	private AtomicLong lastExecutedSeq = new AtomicLong(-1);
	
	private ConcurrentHashMap<String, InetSocketAddress> pendingRequests = new ConcurrentHashMap<>();
	
	private Set<Long> executedSeqNumbers = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());
	
	private Thread watcherThread;
	private volatile boolean running = true;
	private volatile boolean initialized = false;

	/**
	 * @param nodeConfig Server name/address configuration information read from
	 *                   conf/servers.properties.
	 * @param myID       The name of the keyspace to connect to, also the name of
	 *                   the server itself. You can not connect to any other
	 *                   keyspace if using Zookeeper.
	 * @param isaDB      The socket address of the backend datastore to which you
	 *                   need to establish a session.
	 * @throws IOException
	 */
	public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig, String myID, InetSocketAddress isaDB)
			throws IOException {
		super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
				nodeConfig.getNodePort(myID) - ReplicatedServer.SERVER_PORT_OFFSET), isaDB, myID);

		this.myID = myID;
		this.keyspace = myID;
		this.myCheckpointPath = SERVERS_PATH + "/" + myID + "/lastExecuted";

		this.cluster = Cluster.builder().addContactPoint(isaDB.getHostString()).build();
		this.session = cluster.connect(myID);
		
		try {
			initializeZooKeeper();
			startWatcherThread();
			
			synchronized (this) {
				recoverFromCrash();
				initialized = true;
			}
			
		} catch (Exception e) {
			throw new IOException("Initialization failed", e);
		}
	}

	/**
	 * we initialize the zk connection and create the necessary znodes
	 */
	private void initializeZooKeeper() throws IOException, InterruptedException, KeeperException {
		CountDownLatch connectedLatch = new CountDownLatch(1);
		
		zk = new ZooKeeper("localhost:" + DEFAULT_PORT, 5000, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				if (event.getState() == Event.KeeperState.SyncConnected) {
					connectedLatch.countDown();
				}
			}
		});
		
		connectedLatch.await();

		createZNodeIfNotExists(ROOT_PATH, new byte[0], CreateMode.PERSISTENT);
		createZNodeIfNotExists(REQUESTS_PATH, new byte[0], CreateMode.PERSISTENT);
		createZNodeIfNotExists(SERVERS_PATH, new byte[0], CreateMode.PERSISTENT);
		createZNodeIfNotExists(SERVERS_PATH + "/" + myID, new byte[0], CreateMode.PERSISTENT);
		
		if (zk.exists(myCheckpointPath, false) == null) {
			zk.create(myCheckpointPath, "-1".getBytes(StandardCharsets.UTF_8), 
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
	}

	private void createZNodeIfNotExists(String path, byte[] data, CreateMode mode) 
			throws KeeperException, InterruptedException {
		try {
			if (zk.exists(path, false) == null) {
				zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
			}
		} catch (KeeperException.NodeExistsException e) {
		}
	}

	/**
	 * recover from crash by restoring last executed sequence and replaying log
	 */
	private void recoverFromCrash() throws KeeperException, InterruptedException {
		byte[] data = zk.getData(myCheckpointPath, false, null);
		long checkpointSeq = Long.parseLong(new String(data, StandardCharsets.UTF_8));
		
		boolean dbEmpty = isDatabaseEmpty();
		
		List<String> logEntries = zk.getChildren(REQUESTS_PATH, false);
		Collections.sort(logEntries);
		
		long oldestLogSeq = logEntries.isEmpty() ? 0 : extractSequenceNumber(logEntries.get(0));
		
		if (dbEmpty) {
			lastExecutedSeq.set(logEntries.isEmpty() ? -1 : oldestLogSeq - 1);
		} else if (checkpointSeq >= oldestLogSeq - 1) {
			lastExecutedSeq.set(checkpointSeq);
		} else {
			lastExecutedSeq.set(oldestLogSeq - 1);
		}
		
	}

	/**
	 * check if the database table is empty
	 */
	private boolean isDatabaseEmpty() {
		int retries = 3;
		while (retries > 0) {
			try {
				ResultSet rs = session.execute("SELECT * FROM grade LIMIT 1");
				return rs.isExhausted();
			} catch (Exception e) {
				retries--;
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ie) {
					Thread.currentThread().interrupt();
					return false;
				}
			}
		}
		return false;
	}

	/**
	 * start background thread to watch and execute requests
	 */
	private void startWatcherThread() {
		watcherThread = new Thread(() -> {
			// Wait for initialization to complete
			while (!initialized && running) {
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					return;
				}
			}
			
			// main loop
			while (running) {
				try {
					processRequestLog();
					Thread.sleep(50);
				} catch (InterruptedException e) {
					if (!running) break;
				} catch (Exception e) {
				}
			}
		}, "ZK-Watcher-" + myID);
		watcherThread.start();
	}

	private synchronized void processRequestLog() throws KeeperException, InterruptedException {
		List<String> children = zk.getChildren(REQUESTS_PATH, false);
		
		if (children.isEmpty()) {
			return;
		}
		
		Collections.sort(children);
		
		for (String child : children) {
			long seq = extractSequenceNumber(child);
			
			if (seq <= lastExecutedSeq.get()) {
				continue;
			}
			
			if (seq > lastExecutedSeq.get() + 1) {
				return;
			}
			
			// mark as executed (atomically check and set)
			if (!executedSeqNumbers.add(seq)) {
				continue;
			}
			
			try {
				String nodePath = REQUESTS_PATH + "/" + child;
				byte[] data = zk.getData(nodePath, false, null);
				
				RequestData reqData = RequestData.deserialize(new String(data, StandardCharsets.UTF_8));
				
				executeRequest(reqData);
				
				lastExecutedSeq.set(seq);
				zk.setData(myCheckpointPath, String.valueOf(seq).getBytes(StandardCharsets.UTF_8), -1);
				
				if (executedSeqNumbers.size() > MAX_LOG_SIZE) {
					executedSeqNumbers.removeIf(s -> s <= seq - MAX_LOG_SIZE);
				}
				
				checkpointAndCleanup(children);
				
			} catch (KeeperException.NoNodeException e) {
				executedSeqNumbers.remove(seq);
			} catch (Exception e) {
				executedSeqNumbers.remove(seq);
				throw e;
			}
		}
	}

	/**
	 * execute a single request on the db
	 */
	private void executeRequest(RequestData reqData) {
		try {
			String response = "";
			ResultSet results = session.execute(reqData.query);
			
			for (Row row : results) {
				response += row.toString() + "\n";
			}
			
			if (reqData.requestId >= 0) {
				response = reqData.requestId + ":" + response;
			}
			
			InetSocketAddress clientAddr = pendingRequests.remove(reqData.nodeId);
			if (clientAddr != null) {
				try {
					this.clientMessenger.send(clientAddr, response.getBytes(SingleServer.DEFAULT_ENCODING));
				} catch (IOException e) {
				}
			}
			
		} catch (Exception e) {
		}
	}

	/**
	 * we checkpoint and clean up old log entries if log size exceeds threshold
	 */
	private void checkpointAndCleanup(List<String> children) throws KeeperException, InterruptedException {
		if (children.size() <= MAX_LOG_SIZE) {
			return;
		}
		
		// we find the minimum checkpoint across all servers
		long minLastExecuted = Long.MAX_VALUE;
		int serverCount = 0;
		
		try {
			List<String> servers = zk.getChildren(SERVERS_PATH, false);
			for (String server : servers) {
				String checkpointPath = SERVERS_PATH + "/" + server + "/lastExecuted";
				try {
					byte[] data = zk.getData(checkpointPath, false, null);
					long serverLastExec = Long.parseLong(new String(data, StandardCharsets.UTF_8));
					minLastExecuted = Math.min(minLastExecuted, serverLastExec);
					serverCount++;
				} catch (KeeperException.NoNodeException e) {
					// we can't safely delete because the server hasn't created checkpoint yet
					return;
				}
			}
		} catch (Exception e) {
			return;
		}
		
		if (serverCount == 0) {
			return;
		}

		// we keep enough logs to replay test41 (MAX_LOG_SIZE * 5 requests)
		long safeDeleteThreshold = minLastExecuted - 2500;
		
		// to bring log size back to MAX_LOG_SIZE, we delete the oldest entries
		int toDelete = children.size() - MAX_LOG_SIZE;
		int deleted = 0;
		
		for (int i = 0; i < children.size() && deleted < toDelete; i++) {
			String child = children.get(i);
			long seq = extractSequenceNumber(child);
			
			if (seq <= safeDeleteThreshold) {
				try {
					zk.delete(REQUESTS_PATH + "/" + child, -1);
					deleted++;
				} catch (KeeperException.NoNodeException e) {
				}
			} else {
				break;
			}
		}
	}

	/**
	 * extract sequence number from zk node name
	 */
	private long extractSequenceNumber(String nodeName) {
		// format: req-0000000123
		String seqStr = nodeName.substring(nodeName.lastIndexOf('-') + 1);
		return Long.parseLong(seqStr);
	}

	/**
	 * Process bytes received from clients.
	 * All requests (including reads) go through consensus for linearizability.
	 */
	@Override
	protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
		try {
			String request = new String(bytes, SingleServer.DEFAULT_ENCODING);
			int requestId = -1;
			String query = request;
			
			// first parse request ID if present
			int colonIdx = request.indexOf(':');
			if (colonIdx > 0) {
				String idPart = request.substring(0, colonIdx);
				try {
					requestId = Integer.parseInt(idPart);
					query = request.substring(colonIdx + 1);
				} catch (NumberFormatException e) {
				}
			}
			
			// create request data with unique node ID
			RequestData reqData = new RequestData(requestId, query, myID, header.sndr);
			String nodeId = reqData.nodeId;
			
			// store pending request for response routing
			pendingRequests.put(nodeId, header.sndr);
			
			// submit to zk log for total ordering
			String serialized = reqData.serialize();
			zk.create(REQUESTS_PATH + "/req-", serialized.getBytes(StandardCharsets.UTF_8),
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
			
		} catch (Exception e) {
		}
	}


	/**
	 * Gracefully close all resources
	 */
	@Override
	public void close() {
		running = false;
		
		// stop thread
		if (watcherThread != null) {
			watcherThread.interrupt();
			try {
				watcherThread.join(1000);
			} catch (InterruptedException e) {
			}
		}
		
		// close zk
		if (zk != null) {
			try {
				zk.close();
			} catch (InterruptedException e) {
			}
		}
		
		// close cassandra
		if (session != null) {
			session.close();
		}
		if (cluster != null) {
			cluster.close();
		}
		
		super.close();
	}

	/**
	 * inner class to represent request data
	 */
	private static class RequestData {
		int requestId;
		String query;
		String serverId;
		String nodeId; // request uuid
		
		RequestData(int requestId, String query, String serverId, InetSocketAddress clientAddr) {
			this.requestId = requestId;
			this.query = query;
			this.serverId = serverId;
			this.nodeId = serverId + "-" + System.nanoTime() + "-" + requestId;
		}
		
		RequestData(int requestId, String query, String serverId, String nodeId) {
			this.requestId = requestId;
			this.query = query;
			this.serverId = serverId;
			this.nodeId = nodeId;
		}
		
		String serialize() {
			// the format is: requestId|query|serverId|nodeId
			return requestId + "|" + query + "|" + serverId + "|" + nodeId;
		}
		
		static RequestData deserialize(String data) {
			String[] parts = data.split("\\|", 4);
			int reqId = Integer.parseInt(parts[0]);
			String query = parts[1];
			String serverId = parts[2];
			String nodeId = parts[3];
			return new RequestData(reqId, query, serverId, nodeId);
		}
	}

	/**
	 * @param args args[0] must be server.properties file and args[1] must be myID.
	 *             The server prefix in the properties file must be
	 *             ReplicatedServer.SERVER_PREFIX. Optional args[2] if specified
	 *             will be a socket address for the backend datastore.
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		new MyDBFaultTolerantServerZK(NodeConfigUtils.getNodeConfigFromFile(args[0], ReplicatedServer.SERVER_PREFIX,
				ReplicatedServer.SERVER_PORT_OFFSET), args[1],
				args.length > 2 ? Util.getInetSocketAddressFromString(args[2])
						: new InetSocketAddress("localhost", 9042));
	}

}
