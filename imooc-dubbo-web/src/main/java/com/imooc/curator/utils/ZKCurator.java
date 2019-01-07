package com.imooc.curator.utils;

import org.apache.curator.framework.CuratorFramework;

/**
 * Created by helei on 2019/1/5.
 */
public class ZKCurator {

    private CuratorFramework client = null; //zk客户端

    public ZKCurator(CuratorFramework client){
        this.client = client;
    }

    /**
     * 初始化操作
     */
    public void init(){
        client = client.usingNamespace("zk-curator-connector");
    }

    /**
     * 判断zk是否连接
     * @return
     */
    public boolean isZKAlive(){
        return client.isStarted();
    }
}
