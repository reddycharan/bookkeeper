package org.apache.bookkeeper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.apache.bookkeeper.conf.BookKeeperProxyConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandExecutor {
    private final static Logger LOG = LoggerFactory.getLogger(CommandExecutor.class);

    public static final String RUOKCMD = "ruok";

    public boolean execute(String commandString, SocketChannel clientSocketChannel,
            BookKeeperProxyConfiguration bkpConfig) {
        AbstractFourLetterCommand command = getCommand(commandString, clientSocketChannel, bkpConfig);

        if (command == null) {
            return false;
        }
        command.run();
        return true;
    }

    private AbstractFourLetterCommand getCommand(String commandString, SocketChannel clientSocketChannel,
            BookKeeperProxyConfiguration bkpConfig) {
        AbstractFourLetterCommand command = null;
        switch (commandString) {
        case RUOKCMD:
            command = new RuokCommand(clientSocketChannel, bkpConfig);
            break;
        default:
            command = null;
        }
        return command;
    }

    private abstract class AbstractFourLetterCommand {

        protected SocketChannel clientSocketChannel;
        protected BookKeeperProxyConfiguration bkpConfig;

        private AbstractFourLetterCommand(SocketChannel clientSocketChannel, BookKeeperProxyConfiguration bkpConfig) {
            this.clientSocketChannel = clientSocketChannel;
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
        private RuokCommand(SocketChannel clientSocketChannel, BookKeeperProxyConfiguration bkpConfig) {
            super(clientSocketChannel, bkpConfig);
        }

        @Override
        protected void commandRun() throws IOException {
            sendResponse("imok");
        }
    }
}
