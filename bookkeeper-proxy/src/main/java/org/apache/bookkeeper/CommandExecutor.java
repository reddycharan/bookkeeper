package org.apache.bookkeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.BookieInfoReader.BookieInfo;
import org.apache.bookkeeper.conf.BookKeeperProxyConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandExecutor {
    private final static Logger LOG = LoggerFactory.getLogger(CommandExecutor.class);

    public static final String RUOKCMD = "ruok";
    public static final String PVERCMD = "pver";
    public static final String ZKOKCMD = "zkok";
    public static final String BKOKCMD = "bkok";
    public static final String IMOK_STRING = "imok";
    public static final String ZK_CONNECTED_STRING = "connected";
    public static final String ZK_NOTCONNECTED_STRING = "notconnected";
    public static final String ALLBOOKIESOK_STRING = "allbookiesok";
    public static final String SOMEBOOKIESOK_STRING = "somebookiesok";
    public static final String ERROR_STRING = "error";    

    public boolean execute(String commandString, SocketChannel clientSocketChannel,
            BookKeeperProxyConfiguration bkpConfig, BookKeeper bkc, ZooKeeper zkc) {
        AbstractFourLetterCommand command = getCommand(commandString, clientSocketChannel, bkpConfig, bkc, zkc);

        if (command == null) {
            return false;
        }
        command.run();
        return true;
    }

    private AbstractFourLetterCommand getCommand(String commandString, SocketChannel clientSocketChannel,
            BookKeeperProxyConfiguration bkpConfig, BookKeeper bkc, ZooKeeper zkc) {
        AbstractFourLetterCommand command = null;
        switch (commandString) {
        case RUOKCMD:
            command = new RuokCommand(clientSocketChannel);
            break;
        case PVERCMD:
            command = new ProtoVersionCommand(clientSocketChannel);
            break;
        case ZKOKCMD:
            command = new ZkokCommand(clientSocketChannel, zkc);
            break;
        case BKOKCMD:
            command = new BkokCommand(clientSocketChannel, bkc);
            break;
        default:
            command = null;
        }

        if (command != null) {
            command.setBkpConfig(bkpConfig);
        }

        return command;
    }

    private abstract class AbstractFourLetterCommand {

        protected SocketChannel clientSocketChannel;
        protected BookKeeperProxyConfiguration bkpConfig;

        private AbstractFourLetterCommand(SocketChannel clientSocketChannel) {
            this.clientSocketChannel = clientSocketChannel;
        }

        public void setBkpConfig(BookKeeperProxyConfiguration bkpConfig) {
            this.bkpConfig = bkpConfig;
        }

        public void run() {
            try {
                commandRun();
            } catch (IOException ie) {
                LOG.error("Error in running command ", ie);
            }
        }

        protected abstract void commandRun() throws IOException;

        protected void sendResponse(String response) throws IOException {
            clientSocketChannel.write(ByteBuffer.wrap(response.getBytes()));
        }
    }

    private class RuokCommand extends AbstractFourLetterCommand {
        private RuokCommand(SocketChannel clientSocketChannel) {
            super(clientSocketChannel);
        }

        @Override
        protected void commandRun() throws IOException {
            sendResponse(IMOK_STRING);
        }
    }

    private class ProtoVersionCommand extends AbstractFourLetterCommand {
        private ProtoVersionCommand(SocketChannel clientSocketChannel) {
            super(clientSocketChannel);
        }

        @Override
        protected void commandRun() throws IOException {
            sendResponse(Short.toString(BKPConstants.SFS_CURRENT_VERSION));
        }
    }

    private class ZkokCommand extends AbstractFourLetterCommand {
        private final ZooKeeper zkc;

        private ZkokCommand(SocketChannel clientSocketChannel, ZooKeeper zkc) {
            super(clientSocketChannel);
            this.zkc = zkc;
        }

        @Override
        protected void commandRun() throws IOException {
            if (zkc.getState().isConnected()) {
                sendResponse(ZK_CONNECTED_STRING);
            } else {
                sendResponse(ZK_NOTCONNECTED_STRING);
            }
        }
    }
    
    private class BkokCommand extends AbstractFourLetterCommand {
        private final BookKeeper bkc;

        private BkokCommand(SocketChannel clientSocketChannel, BookKeeper bkc) {
            super(clientSocketChannel);
            this.bkc = bkc;
        }

        @Override
        protected void commandRun() throws IOException {
            BookKeeperAdmin bkAdmin = new BookKeeperAdmin(bkc);
            try {
                Collection<BookieSocketAddress> availableBookies = bkAdmin.getAvailableBookies();
                Collection<BookieSocketAddress> readOnlyBookies = bkAdmin.getReadOnlyBookies();
                Set<BookieSocketAddress> allBookies = new HashSet<BookieSocketAddress>(availableBookies);
                allBookies.addAll(readOnlyBookies);

                Map<BookieSocketAddress, BookieInfo> bookieInfoMap = bkc.getBookieInfo();
                Set<BookieSocketAddress> respondedBookies = bookieInfoMap.keySet();

                allBookies.removeAll(respondedBookies);
                if (allBookies.isEmpty()) {
                    sendResponse(ALLBOOKIESOK_STRING);
                } else {
                    LOG.error("Didn't receive BookieInfo response from the following Bookies: {}", allBookies);
                    sendResponse(SOMEBOOKIESOK_STRING);
                }

            } catch (BKException | InterruptedException e) {
                LOG.error("Exception for getBookieInfo call: ", e);
                sendResponse(ERROR_STRING);
            } finally {
                try {
                    bkAdmin.close();
                } catch (InterruptedException | BKException e) {
                    LOG.error("Exception while trying to close BKAdmin: ", e);
                }
            }
        }
    }
}
