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

import java.util.HashSet;
import java.util.Set;

/**
 * Created by go22670 on 1/25/16.
 */
class TypeInfo {
  private int count = 1;
  private float totalWeight = 0f;
  private float prevMax = 0f;
  private Set<Integer> nodes = new HashSet<>();

/*
  public TypeInfo(TypeInfo other) {
    this(other.count, other.totalWeight, other.prevMax);
  }
*/

  /**
   * @param count
   * @param max
   * @param prevMax
   * @see #incrMax(int, float)
   */
  public TypeInfo(int count, float max, float prevMax) {
    this.count = count;
    this.totalWeight = max;
    this.prevMax = prevMax;
  }

  public TypeInfo(float weight, int nodeID) {
    this.totalWeight = weight;
    count = 1;
    nodes.add(nodeID);
  }

  public void max(float weight, int nodeID) {
    boolean newFound = newMax(weight);
    //if (newFound) nodes.add(nodeID);
    nodes.add(nodeID);
    count = getCount() + 1;
  }

  private boolean newMax(float weight) {
    boolean newMax = weight > totalWeight;

    // TODO : correct???
    if (prevMax == 0 && weight == totalWeight) {
      prevMax = totalWeight;
    }
    if (newMax) {
      prevMax = totalWeight;
      totalWeight = weight;
      return true;
    } else {
      return false;
    }
  }

  public int getCount() {
    return count;
  }

  public float getMaxWeight() {
    return totalWeight;
  }

  public float getPrevMax() {
    return prevMax;
  }

  public void incrMax(int otherCount, float otherWeight) {
    //count += otherCount;
    count = otherCount;
    newMax(otherWeight);
  }

  public String toString() {
    return "Count " + getCount() + " : " + getMaxWeight() + " prev " + getPrevMax() + " neighbors " + nodes;
  }

  public Set<Integer> getNodes() {
    return nodes;
  }
}
