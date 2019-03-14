package zookeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class MyWatcher implements Watcher {

	private ZooKeeper connect;
	private String path;

	public MyWatcher(ZooKeeper connect, String path) {
		super();
		this.connect = connect;
		this.path = path;
	}

	@Override
	public void process(WatchedEvent event) {
		// 1打印一下事件
		System.out.println(event);
		// 如果事件类型为none，且连接状态为超时就退出且闭锁减一(实际上还可以监听更多的事件，比如说节点修改、节点删除等事件并作出相应的操作)
		if (event.getType() == Event.EventType.None) {
			switch (event.getState()) {
			case Expired:
				break;
			}

		} else {// 2如果事件类型不是NONE就打印一下事件且重新获取数据并且闭锁减一
			try {
				System.out.println("监听到的事件类型是:" + event.getType());
				// 再次注册监听
				byte[] bn = connect.getData(path, new MyWatcher(connect, path), null);
				String data = new String(bn, "UTF-8");
				System.out.println(data);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}
