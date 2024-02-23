/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.uniffle.storage.handler.impl;

import java.util.List;

import org.apache.uniffle.common.exception.RssFetchFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.BufferSegment;
import org.apache.uniffle.common.ShuffleDataResult;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.storage.handler.api.ClientReadHandler;

public class SingelReplicaClientReadHandler extends AbstractClientReadHandler {

  private static final Logger LOG = LoggerFactory.getLogger(SingelReplicaClientReadHandler.class);

  private final List<ClientReadHandler> handlers;
  private final List<ShuffleServerInfo> shuffleServerInfos;

  private int readHandlerIndex;

  public SingelReplicaClientReadHandler(
      List<ClientReadHandler> handlers,
      List<ShuffleServerInfo> shuffleServerInfos) {
    this.handlers = handlers;
    this.shuffleServerInfos = shuffleServerInfos;
  }

  @Override
  public ShuffleDataResult readShuffleData() {
    ClientReadHandler handler;
    ShuffleDataResult result = null;
    do {
      if (readHandlerIndex >= handlers.size()) {
        return result;
      }
      handler = handlers.get(readHandlerIndex);
      try {
        result = handler.readShuffleData();
      } catch (Exception e) {
        Throwable cause = e.getCause();
        String message = "Failed to read shuffle data from [{}] due to "
                + shuffleServerInfos.get(readHandlerIndex).getId() + ", error: "
                + e.getMessage();
        throw new RssFetchFailedException(message, cause);
      }
      if (result != null && !result.isEmpty()) {
        return result;
      } else {
        LOG.info("Finished read from [{}], but haven't finished read all the blocks.",
                shuffleServerInfos.get(readHandlerIndex).getId());
        readHandlerIndex++;
      }
    } while (true);
  }

  @Override
  public void updateConsumedBlockInfo(BufferSegment bs, boolean isSkippedMetrics) {
    super.updateConsumedBlockInfo(bs, isSkippedMetrics);
    handlers
        .get(Math.min(readHandlerIndex, handlers.size() - 1))
        .updateConsumedBlockInfo(bs, isSkippedMetrics);
  }

  @Override
  public void logConsumedBlockInfo() {
    super.logConsumedBlockInfo();
    handlers.forEach(ClientReadHandler::logConsumedBlockInfo);
  }
}
