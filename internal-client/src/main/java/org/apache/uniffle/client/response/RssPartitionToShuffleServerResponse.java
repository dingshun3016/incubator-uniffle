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

package org.apache.uniffle.client.response;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.rpc.StatusCode;
import org.apache.uniffle.proto.RssProtos;

public class RssPartitionToShuffleServerResponse extends ClientResponse {

  private Map<Integer, List<ShuffleServerInfo>> partitionToServers;
  private Map<Integer, Map<Integer, List<ShuffleServerInfo>>> failoverPartitionServers;
  private Set<ShuffleServerInfo> shuffleServersForData;
  private RemoteStorageInfo remoteStorageInfo;

  public RssPartitionToShuffleServerResponse(
          StatusCode statusCode,
          String message,
          Map<Integer, List<ShuffleServerInfo>> partitionToServers,
          Map<Integer, Map<Integer, List<ShuffleServerInfo>>> failoverPartitionServers,
          Set<ShuffleServerInfo> shuffleServersForData,
          RemoteStorageInfo remoteStorageInfo) {
    super(statusCode, message);
    this.partitionToServers = partitionToServers;
    this.failoverPartitionServers = failoverPartitionServers;
    this.remoteStorageInfo = remoteStorageInfo;
    this.shuffleServersForData = shuffleServersForData;
  }

  public Map<Integer, List<ShuffleServerInfo>> getPartitionToServers() {
    return partitionToServers;
  }

  public Map<Integer, Map<Integer, List<ShuffleServerInfo>>> getFailoverPartitionServers() {
    return failoverPartitionServers;
  }

  public Set<ShuffleServerInfo> getShuffleServersForData() {
    return shuffleServersForData;
  }

  public RemoteStorageInfo getRemoteStorageInfo() {
    return remoteStorageInfo;
  }

  public static RssPartitionToShuffleServerResponse fromProto(
      RssProtos.PartitionToShuffleServerResponse response) {
    Map<Integer, RssProtos.GetShuffleServerListResponse> partitionToShuffleServerMap =
        response.getPartitionToShuffleServerMap();
    Map<Integer, List<ShuffleServerInfo>> rpcPartitionToShuffleServerInfos = Maps.newHashMap();
    Set<Map.Entry<Integer, RssProtos.GetShuffleServerListResponse>> entries =
        partitionToShuffleServerMap.entrySet();
    for (Map.Entry<Integer, RssProtos.GetShuffleServerListResponse> entry : entries) {
      Integer partitionId = entry.getKey();
      List<ShuffleServerInfo> shuffleServerInfos = Lists.newArrayList();
      List<? extends RssProtos.ShuffleServerIdOrBuilder> serversOrBuilderList =
          entry.getValue().getServersOrBuilderList();
      for (RssProtos.ShuffleServerIdOrBuilder shuffleServerIdOrBuilder : serversOrBuilderList) {
        shuffleServerInfos.add(
            new ShuffleServerInfo(
                shuffleServerIdOrBuilder.getId(),
                shuffleServerIdOrBuilder.getIp(),
                shuffleServerIdOrBuilder.getPort(),
                shuffleServerIdOrBuilder.getNettyPort()));
      }

      rpcPartitionToShuffleServerInfos.put(partitionId, shuffleServerInfos);
    }
    Set<ShuffleServerInfo> rpcShuffleServersForData = Sets.newHashSet();
    for (List<ShuffleServerInfo> ssis : rpcPartitionToShuffleServerInfos.values()) {
      rpcShuffleServersForData.addAll(ssis);
    }

    Map<Integer, Map<Integer, List<ShuffleServerInfo>>> rpcPartitionToFailoverShuffleServerInfos = Maps.newHashMap();
    Map<Integer, RssProtos.GetFailoverShuffleServerListResponse> failoverPartitionToShuffleServerMap =
            response.getFailoverPartitionToShuffleServerMap();
    Set<Map.Entry<Integer, RssProtos.GetFailoverShuffleServerListResponse>> failoverPartitionEntries =
            failoverPartitionToShuffleServerMap.entrySet();
    for (Map.Entry<Integer, RssProtos.GetFailoverShuffleServerListResponse> entry : failoverPartitionEntries) {
      Integer partitionId = entry.getKey();
      Map<Integer, RssProtos.GetShuffleServerListResponse> replicaToShuffleServerMap =
              entry.getValue().getReplicaToShuffleServerMap();

      Map<Integer, List<ShuffleServerInfo>> rpcReplicaToFailoverShuffleServerInfos = Maps.newHashMap();
      for (Map.Entry<Integer, RssProtos.GetShuffleServerListResponse> replicaEntry : replicaToShuffleServerMap.entrySet()) {
        Integer replicaIdx = replicaEntry.getKey();
        List<ShuffleServerInfo> replicaShuffleServerInfos = Lists.newArrayList();
        List<? extends RssProtos.ShuffleServerIdOrBuilder> replicaServersOrBuilderList =
                replicaEntry.getValue().getServersOrBuilderList();
        for (RssProtos.ShuffleServerIdOrBuilder shuffleServerIdOrBuilder : replicaServersOrBuilderList) {
          replicaShuffleServerInfos.add(
                  new ShuffleServerInfo(
                          shuffleServerIdOrBuilder.getId(),
                          shuffleServerIdOrBuilder.getIp(),
                          shuffleServerIdOrBuilder.getPort(),
                          shuffleServerIdOrBuilder.getNettyPort()));
        }
        rpcReplicaToFailoverShuffleServerInfos.put(replicaIdx, replicaShuffleServerInfos);
      }
      rpcPartitionToFailoverShuffleServerInfos.put(partitionId, rpcReplicaToFailoverShuffleServerInfos);
    }

    RssProtos.RemoteStorageInfo protoRemoteStorageInfo = response.getRemoteStorageInfo();
    RemoteStorageInfo rpcRemoteStorageInfo =
        new RemoteStorageInfo(
            protoRemoteStorageInfo.getPath(), protoRemoteStorageInfo.getConfItemsMap());
    return new RssPartitionToShuffleServerResponse(
        StatusCode.valueOf(response.getStatus().name()),
        response.getMsg(),
        rpcPartitionToShuffleServerInfos,
        rpcPartitionToFailoverShuffleServerInfos,
        rpcShuffleServersForData,
        rpcRemoteStorageInfo);
  }
}
