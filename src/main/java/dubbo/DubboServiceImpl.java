package dubbo;

public class DubboServiceImpl implements DubboService {
	public String sayHello(String name) {
		return "Hello " + name;
	}
}
