package learn.soft.kafka;


import java.util.Properties;

import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import learn.soft.utils.ConfigUtil;

/**
 * 生产者连接池单例实现类
 * 
 * @author gy
 * 
 */
public class ProducerPool extends PoolBase<Producer<Object, Object>> implements
		ConnectionPool<Producer<Object, Object>> {
	
	/**
	 * 日志
	 */
	private static final Logger LOGGER=LoggerFactory.getLogger(ProducerPool.class);

	/**
	 * 序列化标识
	 */
	private static final long serialVersionUID = 8632632508741255736L;

	/**
	 * 初始化
	 */
	static {

		/**
		 * 初始化连接池配置信息
		 */
		ProducerPool.poolConfig = new PoolConfig();
		// 连接池最大连接数
		ProducerPool.poolConfig.setMaxTotal(20);
		// 连接池最大空闲连接数
		ProducerPool.poolConfig.setMaxIdle(5);
		// 获取连接时最大等待时长
		ProducerPool.poolConfig.setMaxWaitMillis(1000);
		// 获取连接时检查有效性
		ProducerPool.poolConfig.setTestOnBorrow(true);

		try {
			
			/**
			 * 初始化生产者配置信息
			 */
			ProducerPool.producerConfig = ConfigUtil.getProps("kafka-producer.properties");
			
		} catch (Exception e) {
			LOGGER.error("发生错误：生产者配置文件kafka-producer.properties不存在");
		}

	}

	/**
	 * 私有静态实例
	 */
	private static ProducerPool instance;

	/**
	 * 连接池配置信息
	 */
	private static PoolConfig poolConfig;

	/**
	 * 生产者配置信息
	 */
	private static Properties producerConfig;

	/**
	 * 私有化构造方法
	 */
	private ProducerPool() {
		super(poolConfig, new ProducerFactory(producerConfig));
	}

	/**
	 * 静态实例化方法
	 * 
	 * @return ProducerPool 生产者连接池单例对象
	 */
	public static ProducerPool getInstance() {
		if (null == instance) {
			synchronized (ProducerPool.class) {
				if (null == instance)
					instance = new ProducerPool();
			}
		}
		return instance;

	}

	@Override
	public Producer<Object, Object> getConnection() {
		return super.getResource();
	}

	@Override
	public void returnConnection(Producer<Object, Object> conn) {
		super.returnResource(conn);
	}

	@Override
	public void invalidateConnection(Producer<Object, Object> conn) {
		super.invalidateResource(conn);
	}

}
