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

package org.apache.uniffle.storage.factory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.api.ShuffleServerClient;
import org.apache.uniffle.client.factory.ShuffleServerClientFactory;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.RssUtils;
import org.apache.uniffle.storage.handler.api.ClientReadHandler;
import org.apache.uniffle.storage.handler.api.ShuffleDeleteHandler;
import org.apache.uniffle.storage.handler.impl.*;
import org.apache.uniffle.storage.request.CreateShuffleDeleteHandlerRequest;
import org.apache.uniffle.storage.request.CreateShuffleReadHandlerRequest;
import org.apache.uniffle.storage.util.StorageType;

public class ShuffleHandlerFactory {

  private static ShuffleHandlerFactory INSTANCE;

  private ShuffleHandlerFactory() {}

  public static synchronized ShuffleHandlerFactory getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new ShuffleHandlerFactory();
    }
    return INSTANCE;
  }

  public ClientReadHandler createShuffleReadHandler(CreateShuffleReadHandlerRequest request) {
    if (CollectionUtils.isEmpty(request.getShuffleServerInfoList())) {
      throw new RssException("Shuffle servers should not be empty!");
    }
    Map<Integer, List<ShuffleServerInfo>> failoverShuffleServerInfoList =
        request.getFailoverShuffleServerInfoList();
    if (request.getShuffleServerInfoList().size() > 1) {
      List<ClientReadHandler> handlers = Lists.newArrayList();
      List<ShuffleServerInfo> shuffleServerInfoList = request.getShuffleServerInfoList();
      for (int i = 0; i < shuffleServerInfoList.size(); i++) {
        List<ShuffleServerInfo> replicaShuffleServerInfos = Lists.newArrayList();
        replicaShuffleServerInfos.add(shuffleServerInfoList.get(i));
        if (failoverShuffleServerInfoList != null && failoverShuffleServerInfoList.size() > i) {
          replicaShuffleServerInfos.addAll(failoverShuffleServerInfoList.get(i));
        }
        handlers.add(
            ShuffleHandlerFactory.getInstance()
                .createSingleReplicaForMutiServerClientReadHandler(
                    request, replicaShuffleServerInfos));
      }
      return new MultiReplicaClientReadHandler(
          handlers,
          request.getShuffleServerInfoList(),
          request.getExpectBlockIds(),
          request.getProcessBlockIds());
    } else {
      List<ShuffleServerInfo> shuffleServerInfoList = request.getShuffleServerInfoList();
      if (failoverShuffleServerInfoList != null && !failoverShuffleServerInfoList.isEmpty()) {
        shuffleServerInfoList.addAll(failoverShuffleServerInfoList.get(0));
      }
      return createSingleReplicaForMutiServerClientReadHandler(request, shuffleServerInfoList);
    }
  }

  public ClientReadHandler createSingleReplicaClientReadHandler(
      CreateShuffleReadHandlerRequest request, ShuffleServerInfo serverInfo) {
    String storageType = request.getStorageType();
    StorageType type = StorageType.valueOf(storageType);

    if (StorageType.MEMORY == type) {
      throw new UnsupportedOperationException(
          "Doesn't support storage type for client read  :" + storageType);
    }

    if (StorageType.HDFS == type) {
      return getHadoopClientReadHandler(request, serverInfo);
    }
    if (StorageType.LOCALFILE == type) {
      return getLocalfileClientReaderHandler(request, serverInfo);
    }

    List<Supplier<ClientReadHandler>> handlers = new ArrayList<>();
    if (StorageType.withMemory(type)) {
      handlers.add(() -> getMemoryClientReadHandler(request, serverInfo));
    }
    if (StorageType.withLocalfile(type)) {
      handlers.add(() -> getLocalfileClientReaderHandler(request, serverInfo));
    }
    if (StorageType.withHadoop(type)) {
      handlers.add(() -> getHadoopClientReadHandler(request, serverInfo));
    }
    if (handlers.isEmpty()) {
      throw new RssException(
          "This should not happen due to the unknown storage type: " + storageType);
    }

    return new ComposedClientReadHandler(serverInfo, handlers);
  }

  public ClientReadHandler createSingleReplicaForMutiServerClientReadHandler(
      CreateShuffleReadHandlerRequest request,
      List<ShuffleServerInfo> singleReplicaShuffleServerInfoList) {
    List<ClientReadHandler> handlers = Lists.newArrayList();
    singleReplicaShuffleServerInfoList.forEach(
        (ssi) -> {
          handlers.add(
              ShuffleHandlerFactory.getInstance()
                  .createSingleReplicaClientReadHandler(request, ssi));
        });
    return new SingelReplicaClientReadHandler(
        handlers,
        request.getShuffleServerInfoList());
  }

  private ClientReadHandler getMemoryClientReadHandler(
      CreateShuffleReadHandlerRequest request, ShuffleServerInfo ssi) {
    ShuffleServerClient shuffleServerClient =
        ShuffleServerClientFactory.getInstance()
            .getShuffleServerClient(request.getClientType().name(), ssi, request.getClientConf());
    Roaring64NavigableMap expectTaskIds = null;
    if (request.isExpectedTaskIdsBitmapFilterEnable()) {
      Roaring64NavigableMap realExceptBlockIds = RssUtils.cloneBitMap(request.getExpectBlockIds());
      realExceptBlockIds.xor(request.getProcessBlockIds());
      expectTaskIds = RssUtils.generateTaskIdBitMap(realExceptBlockIds, request.getIdHelper());
    }
    ClientReadHandler memoryClientReadHandler =
        new MemoryClientReadHandler(
            request.getAppId(),
            request.getShuffleId(),
            request.getPartitionId(),
            request.getReadBufferSize(),
            shuffleServerClient,
            expectTaskIds);
    return memoryClientReadHandler;
  }

  private ClientReadHandler getLocalfileClientReaderHandler(
      CreateShuffleReadHandlerRequest request, ShuffleServerInfo ssi) {
    ShuffleServerClient shuffleServerClient =
        ShuffleServerClientFactory.getInstance()
            .getShuffleServerClient(request.getClientType().name(), ssi, request.getClientConf());
    return new LocalFileClientReadHandler(
        request.getAppId(),
        request.getShuffleId(),
        request.getPartitionId(),
        request.getIndexReadLimit(),
        request.getPartitionNumPerRange(),
        request.getPartitionNum(),
        request.getReadBufferSize(),
        request.getExpectBlockIds(),
        request.getProcessBlockIds(),
        shuffleServerClient,
        request.getDistributionType(),
        request.getExpectTaskIds());
  }

  private ClientReadHandler getHadoopClientReadHandler(
      CreateShuffleReadHandlerRequest request, ShuffleServerInfo ssi) {
    return new HadoopClientReadHandler(
        request.getAppId(),
        request.getShuffleId(),
        request.getPartitionId(),
        request.getIndexReadLimit(),
        request.getPartitionNumPerRange(),
        request.getPartitionNum(),
        request.getReadBufferSize(),
        request.getExpectBlockIds(),
        request.getProcessBlockIds(),
        request.getStorageBasePath(),
        request.getHadoopConf(),
        request.getDistributionType(),
        request.getExpectTaskIds(),
        ssi.getId(),
        request.isOffHeapEnabled());
  }

  public ShuffleDeleteHandler createShuffleDeleteHandler(
      CreateShuffleDeleteHandlerRequest request) {
    if (StorageType.HDFS.name().equals(request.getStorageType())) {
      return new HadoopShuffleDeleteHandler(request.getConf());
    } else if (StorageType.LOCALFILE.name().equals(request.getStorageType())) {
      return new LocalFileDeleteHandler();
    } else {
      throw new UnsupportedOperationException(
          "Doesn't support storage type for shuffle delete handler:" + request.getStorageType());
    }
  }
}
