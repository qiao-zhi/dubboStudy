package zookeper;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

public class CuratorAPI {
	private static final String URL = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";
	private static String path = "/t7";

	public static void main(String[] args) throws Exception {
		CuratorFramework client = getClient();
		client.start();
		// setListenterThreeThree(client);
		// setListenterThreeTwo(client);
		setListenterThreeOne(client);
		// setListenterTwo(client);
		
		Thread.sleep(500 * 1000);
	}

	/**
	 * 一次性监听第一种方法
	 * 
	 * @param client
	 * @throws Exception
	 */
	private static void setListenterOne(CuratorFramework client) throws Exception {
		// 注册观察者，当节点变动时触发
		byte[] data = client.getData().usingWatcher(new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				System.out.println(event.getType());
			}
		}).forPath("/t4");
	}

	/**
	 * 一次性监听第二种方法
	 * 
	 * @param client
	 * @throws Exception
	 */
	private static void setListenterTwo(CuratorFramework client) throws Exception {

		ExecutorService pool = Executors.newCachedThreadPool();

		CuratorListener listener = new CuratorListener() {
			@Override
			public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
				System.out.println("监听器  : " + event.toString());
			}
		};
		client.getCuratorListenable().addListener(listener, pool);
	}

	/**
	 * 
	 * @param client
	 * @throws Exception
	 */
	private static void setListenterThreeOne(CuratorFramework client) throws Exception {
		PathChildrenCache childrenCache = new PathChildrenCache(client, path, true);
		PathChildrenCacheListener childrenCacheListener = new PathChildrenCacheListener() {
			@Override
			public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
				System.out.println("事件类型"+event.getType());
				ChildData data = event.getData();
				switch (event.getType()) {
				case CHILD_ADDED:
					System.out.println("CHILD_ADDED : " + data.getPath() + "  数据:" + data.getData());
					break;
				case CHILD_REMOVED:
					System.out.println("CHILD_REMOVED : " + data.getPath() + "  数据:" + data.getData());
					break;
				case CHILD_UPDATED:
					System.out.println("CHILD_UPDATED : " + data.getPath() + "  数据:" + data.getData());
					break;
				default:
					break;
				}
			}
		};

		// 在注册监听器的时候，如果传入此参数，当事件触发时，逻辑由线程池处理。如果不传会采用默认的线程池
		ExecutorService pool = Executors.newFixedThreadPool(2);
		// childrenCache.getListenable().addListener(childrenCacheListener);
		childrenCache.getListenable().addListener(childrenCacheListener, pool);
		// 设置监听模式
		childrenCache.start();
	}

	// 监听本节点的变化 节点可以进行修改操作 删除节点后会再次创建(空节点)
	private static void setListenterThreeTwo(CuratorFramework client) throws Exception {
		ExecutorService pool = Executors.newCachedThreadPool();
		// 设置节点的cache
		final NodeCache nodeCache = new NodeCache(client, path, false);
		nodeCache.getListenable().addListener(new NodeCacheListener() {
			@Override
			public void nodeChanged() throws Exception {
				System.out.println("the test node is change and result is :");
				System.out.println("path : " + nodeCache.getCurrentData().getPath());
				System.out.println("data : " + new String(nodeCache.getCurrentData().getData()));
				System.out.println("stat : " + nodeCache.getCurrentData().getStat());
			}
		});
		nodeCache.start();
	}

	// 监控 指定节点和节点下的所有的节点的变化--无限监听
	private static void setListenterThreeThree(CuratorFramework client) throws Exception {
		// 设置节点的cache
		TreeCache treeCache = new TreeCache(client, path);
		// 设置监听器和处理过程
		treeCache.getListenable().addListener(new TreeCacheListener() {
			@Override
			public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
				ChildData data = event.getData();
				if (data != null) {
					switch (event.getType()) {
					case NODE_ADDED:
						System.out.println("NODE_ADDED : " + data.getPath() + "  数据:" + new String(data.getData()));
						break;
					case NODE_REMOVED:
						System.out.println("NODE_REMOVED : " + data.getPath() + "  数据:" + new String(data.getData()));
						break;
					case NODE_UPDATED:
						System.out.println("NODE_UPDATED : " + data.getPath() + "  数据:" + new String(data.getData()));
						break;

					default:
						break;
					}
				} else {
					System.out.println("data is null : " + event.getType());
				}
			}
		});
		// 开始监听
		treeCache.start();
	}

	private static void asyncCreate(CuratorFramework client) throws Exception {
		client.create().inBackground(new BackgroundCallback() {
			@Override
			public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
				System.out.println(curatorEvent.getType() + "   " + curatorEvent.getResultCode());
			}
		}, Executors.newFixedThreadPool(2)).forPath("/t2", "测试值".getBytes());
	}

	private static void getChildren(CuratorFramework client) throws Exception {
		List<String> forPath = client.getChildren().forPath("/");
		System.out.println(forPath);
	}

	private static void isExists(CuratorFramework client) throws Exception {
		Stat forPath = client.checkExists().forPath("/t4");
		if (forPath != null) {
			System.out.println("exists");
		} else {
			System.out.println("not exists");
		}
	}

	private static void update(CuratorFramework client) throws Exception {
		// 更新数据，返回的是stat
		Stat forPath = client.setData().forPath("/t4", "data".getBytes());

		// 更新一个节点的数据内容，强制指定版本进行更新
		Stat stat = new Stat();
		client.getData().storingStatIn(stat).forPath("/t4");
		Stat forPath2 = client.setData().withVersion(stat.getVersion()).forPath("/t4", "data222".getBytes());
	}

	private static void getData(CuratorFramework client) throws Exception, UnsupportedEncodingException {
		// 读取数据不获取stat
		byte[] forPath = client.getData().forPath("/t4");
		System.out.println(new String(forPath, "UTF-8"));

		// 读取数据且获取stat
		Stat stat = new Stat();
		byte[] forPath2 = client.getData().storingStatIn(stat).forPath("/t4");
		System.out.println(new String(forPath2, "UTF-8"));
		System.out.println(stat);

		// 注册观察者，当节点变动时触发
		byte[] data = client.getData().usingWatcher(new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				System.out.println(event.getType());
			}
		}).forPath("/t4");
		System.out.println("/t4: " + new String(data));
	}

	private static void delete(CuratorFramework client) throws Exception {
		// 删除子节点，只能删除叶子节点
		client.delete().forPath("/t2");
		// 递归删除
		client.delete().deletingChildrenIfNeeded().forPath("/t4/t41");
		// 指定版本进行删除
		client.delete().withVersion(0).forPath("/t1");
		// 强制删除。guaranteed()接口是一个保障措施，只要客户端会话有效，那么Curator会在后台持续进行删除操作，直到删除节点成功。
		client.delete().guaranteed().forPath("/t30000000002");
	}

	private static void createNode(CuratorFramework client) throws Exception {
		// 创建普通节点(默认是持久节点),内容为空
		client.create().forPath("/t1");
		// 创建普通节点(默认是持久节点)
		client.create().forPath("/t2", "123456".getBytes());
		// 创建临时顺序节点
		client.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath("/t3", "123456".getBytes());
		// 地柜创建，如果父节点不存在也会创建
		client.create().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL)
				.forPath("/t4/t41/t411", "123456".getBytes());
	}

	private static CuratorFramework getClient() {
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework client = CuratorFrameworkFactory.builder().connectString(URL).sessionTimeoutMs(5000)
				.connectionTimeoutMs(5000).retryPolicy(retryPolicy).namespace("curator").build();
		return client;
	}
}
