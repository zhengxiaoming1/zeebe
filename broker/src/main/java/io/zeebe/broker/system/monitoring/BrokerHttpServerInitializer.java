/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.monitoring;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import io.prometheus.client.CollectorRegistry;

public final class BrokerHttpServerInitializer extends ChannelInitializer<SocketChannel> {

  private static final HttpServerCodec HTTP_SERVER_CODEC = new HttpServerCodec();
  private final BrokerHttpServerHandler channelHandler;

  public BrokerHttpServerInitializer(
      final CollectorRegistry metricsRegistry,
      final BrokerHealthCheckService brokerHealthCheckService) {
    this.channelHandler = new BrokerHttpServerHandler(metricsRegistry,
        brokerHealthCheckService);
  }

  @Override
  protected void initChannel(final SocketChannel ch) {
    ch.pipeline()
        .addLast("codec", HTTP_SERVER_CODEC)
        .addLast("request", channelHandler);
  }
}
