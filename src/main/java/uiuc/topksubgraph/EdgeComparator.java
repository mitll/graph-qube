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

import java.util.Comparator;

/**
 * @see MultipleIndexConstructor#populateSortedEdgeLists(Graph)
 */
class EdgeComparator implements Comparator<Edge> {
  @Override
  public int compare(Edge e1, Edge e2) {
    double e1wt = e1.getWeight();
    double e2wt = e2.getWeight();
    int comp = 0;
    if (e1wt > e2wt)
      comp = -1;
    else if (e1wt < e2wt)
      comp = 1;

    if (comp == 0) {
      comp = Integer.valueOf(e1.getSrc()).compareTo(e1.getSrc());
    }
    if (comp == 0) {
      comp = Integer.valueOf(e1.getDst()).compareTo(e1.getDst());
    }
    return comp;
  }
}
