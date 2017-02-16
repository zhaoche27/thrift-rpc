package com.ttyc.datagroup.commons.zookeeper.zkclient;

import com.ttyc.datagroup.commons.zookeeper.AbstractZookeeperClient;
import com.ttyc.datagroup.commons.zookeeper.ChildListener;
import com.ttyc.datagroup.commons.zookeeper.StateListener;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.zookeeper.Watcher;

import java.util.List;

/**
 * Created by zhaoche on 2017/2/4.
 */
public class ZKClientZookeeperClient extends AbstractZookeeperClient<IZkChildListener> {

    private final ZkClient client;

    private volatile Watcher.Event.KeeperState state = Watcher.Event.KeeperState.SyncConnected;

    public ZKClientZookeeperClient(String zkConnectionString) {
        super(zkConnectionString);
        client = new ZkClient(zkConnectionString);
        client.subscribeStateChanges(new IZkStateListener() {
            public void handleStateChanged(Watcher.Event.KeeperState state) throws Exception {
                ZKClientZookeeperClient.this.state = state;
                if (state == Watcher.Event.KeeperState.Disconnected) {
                    stateChanged(StateListener.DISCONNECTED);
                } else if (state == Watcher.Event.KeeperState.SyncConnected) {
                    stateChanged(StateListener.CONNECTED);
                }
            }

            public void handleNewSession() throws Exception {
                stateChanged(StateListener.RECONNECTED);
            }

            public void handleSessionEstablishmentError(Throwable error) throws Exception {

            }
        });
    }

    public void createPersistent(String path, String data) {
        try {
            client.createPersistent(path, true);
            if (data != null) {
                client.writeData(path, data);
            }
        } catch (ZkNodeExistsException e) {
        }
    }

    public void createEphemeral(String path, String data) {
        try {
            client.createEphemeral(path, data);
        } catch (ZkNodeExistsException e) {
        }
    }

    @Override
    public void writeData(String path, String data) {
        client.writeData(path, data);
    }

    public void delete(String path) {
        try {
            client.delete(path);
        } catch (ZkNoNodeException e) {
        }
    }

    public List<String> getChildren(String path) {
        try {
            return client.getChildren(path);
        } catch (ZkNoNodeException e) {
            return null;
        }
    }

    public boolean isConnected() {
        return state == Watcher.Event.KeeperState.SyncConnected;
    }

    @Override
    public String getData(String path) {
        return client.readData(path, true);
    }

    public void doClose() {
        if (client != null) {
            client.close();
        }
    }

    public IZkChildListener createTargetChildListener(String path, final ChildListener listener) {
        return new IZkChildListener() {
            public void handleChildChange(String parentPath, List<String> currentChilds)
                    throws Exception {
                listener.childChanged(parentPath, currentChilds);
            }
        };
    }

    public List<String> addTargetChildListener(String path, final IZkChildListener listener) {
        return client.subscribeChildChanges(path, listener);
    }

    public void removeTargetChildListener(String path, IZkChildListener listener) {
        client.unsubscribeChildChanges(path, listener);
    }
}
