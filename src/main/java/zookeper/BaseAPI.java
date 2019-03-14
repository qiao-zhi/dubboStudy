package zookeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class BaseAPI {
	private static ZooKeeper zoo;
	final static CountDownLatch connectedSignal = new CountDownLatch(1);

	public static ZooKeeper connect(String host) throws IOException, InterruptedException {
		zoo = new ZooKeeper(host, 5000, new Watcher() {
			public void process(WatchedEvent event) {
				if (event.getState() == KeeperState.SyncConnected) {
					connectedSignal.countDown();
				}
			}
		});

		connectedSignal.await();
		return zoo;
	}

	public void close() throws InterruptedException {
		zoo.close();
	}

	public static void create(String path, byte[] data) throws KeeperException, InterruptedException {
		zoo.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}

	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		final String path = "/t10";
		final ZooKeeper connect = connect("127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183");

		// 会话添加用户和密码信息
		connect.addAuthInfo("digest", "user:123456".getBytes());

		byte[] data = connect.getData(path, false, null);
		System.out.println(new String(data, "UTF-8"));
	}
}
