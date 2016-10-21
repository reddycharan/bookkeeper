package org.apache.bookkeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.apache.bookkeeper.conf.BookKeeperProxyConfiguration;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandExecutor {
    private final static Logger LOG = LoggerFactory.getLogger(CommandExecutor.class);

    public static final String RUOKCMD = "ruok";
    public static final String PVERCMD = "pver";
    public static final String ZKOKCMD = "zkok";
    public static final String ZK_CONNECTED_STRING = "connected";
    public static final String ZK_NOTCONNECTED_STRING = "notconnected";

    public boolean execute(String commandString, SocketChannel clientSocketChannel,
            BookKeeperProxyConfiguration bkpConfig, ZooKeeper zkc) {
        AbstractFourLetterCommand command = getCommand(commandString, clientSocketChannel, bkpConfig, zkc);

        if (command == null) {
            return false;
        }
        command.run();
        return true;
    }

    private AbstractFourLetterCommand getCommand(String commandString, SocketChannel clientSocketChannel,
            BookKeeperProxyConfiguration bkpConfig, ZooKeeper zkc) {
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
            sendResponse("imok");
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
}
