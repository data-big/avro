package learn.soft.kafka;


import java.io.Serializable;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * 默认池配置
 *
 * @author gy
 *
 */
public class PoolConfig extends GenericObjectPoolConfig implements Serializable {
	
	/**
	 * 序列化标识
	 */
	private static final long serialVersionUID = -2414567557372345057L;

	/**
	 * 默认构造方法
	 */
	public PoolConfig() {
		//空闲时检查连接有效性
		setTestWhileIdle(true);
		//逐出连接最小空闲时间
		setMinEvictableIdleTimeMillis(60000);
		//逐出连接扫描间隔
		setTimeBetweenEvictionRunsMillis(30000);
		//每次逐出检查时 逐出的最大数目 如果为负数就是 : 1/abs(n)
		setNumTestsPerEvictionRun(-1);
	}

}
