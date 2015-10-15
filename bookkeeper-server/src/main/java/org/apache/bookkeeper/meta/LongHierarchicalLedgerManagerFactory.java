package org.apache.bookkeeper.meta;

import java.io.IOException;
import java.util.List;

import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;

public class LongHierarchicalLedgerManagerFactory extends HierarchicalLedgerManagerFactory {

    public static final String NAME = "longhierarchical";

    @Override
    public LedgerManager newLedgerManager() {
        return new LongHierarchicalLedgerManager(conf, zk);
    }

}
