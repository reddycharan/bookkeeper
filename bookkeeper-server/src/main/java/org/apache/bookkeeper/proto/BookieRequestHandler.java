/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.proto;

import java.nio.channels.ClosedChannelException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.processor.RequestProcessor;
import org.apache.bookkeeper.proto.BookkeeperProtocol.OperationType;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serverside handler for bookkeeper requests
 */
class BookieRequestHandler extends SimpleChannelHandler {

    private final static Logger LOG = LoggerFactory.getLogger(BookieRequestHandler.class);
    private final static String auditTag = " - AUDIT - ";
    private final RequestProcessor requestProcessor;
    private final ChannelGroup allChannels;

    BookieRequestHandler(ServerConfiguration conf, RequestProcessor processor, ChannelGroup allChannels) {
	this.requestProcessor = processor;
	this.allChannels = allChannels;
    }

    @Override
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
	allChannels.add(ctx.getChannel());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
	Throwable throwable = e.getCause();
	if (throwable instanceof ClosedChannelException) {
	    LOG.info("Client died before request could be completed", throwable);
	    return;
	}
	LOG.error(auditTag + "Unhandled exception occurred in I/O thread or handler. Connection: {}" + "\tException: {}", e, throwable.getMessage());
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
	LOG.info(auditTag + "Channel connected {}", e);
    }

    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
	LOG.info(auditTag + "Channel disconnected {}.", e);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
	Object event = e.getMessage();
	if (!(event instanceof BookkeeperProtocol.Request || event instanceof BookieProtocol.Request)) {
	    ctx.sendUpstream(e);
	    return;
	}
	requestProcessor.processRequest(event, ctx.getChannel());
    }

}
