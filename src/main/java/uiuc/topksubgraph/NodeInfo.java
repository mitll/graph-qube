/*
 * Copyright 2013-2016 MIT Lincoln Laboratory, Massachusetts Institute of Technology
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uiuc.topksubgraph;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by go22670 on 1/25/16.
 */
class NodeInfo {
  private final long nodeID;
  private final Map<String, TypeInfo> typeToInfo = new HashMap<>();

  NodeInfo(long nodeID) {
    this.nodeID = nodeID;
  }

  /**
   * @param type
   * @param weight
   * @param nodeID
   * @see #computeIndicesFast(Graph)
   */
  public void addWeight(String type, float weight, long nodeID) {
    TypeInfo info = typeToInfo.get(type);
    if (info == null) {
      info = new TypeInfo(weight, nodeID);
      typeToInfo.put(type, info);
    } else {
      info.max(weight, nodeID);
    }
  }

  public void incrMax(String type, int typeCountOfNeighbor, float weightOfNeighbor) {
    TypeInfo info = typeToInfo.get(type);
    if (info == null) {
      info = new TypeInfo(typeCountOfNeighbor, weightOfNeighbor, weightOfNeighbor);
      typeToInfo.put(type, info);
    } else {
      info.incrMax(typeCountOfNeighbor, weightOfNeighbor);
    }
  }

  public String toString() {
    return typeToInfo.toString();
  }

  long getNodeID() {
    return nodeID;
  }

  Map<String, TypeInfo> getTypeToInfo() {
    return typeToInfo;
  }
}
