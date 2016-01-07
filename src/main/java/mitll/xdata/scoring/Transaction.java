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

package mitll.xdata.scoring;

import java.util.Arrays;

public class Transaction {
    private String source;
    private String target;

    /** UNIX time in milliseconds */
    private long time;
    private double[] features;

  /**
   * @see mitll.xdata.dataset.bitcoin.binding.BitcoinBinding#createFeatureVectors
   * @see HmmScorer#createEndTransaction(int)
   * @param source
   * @param target
   * @param time
   * @param features
   */
    public Transaction(String source, String target, long time, double[] features) {
        this.source = source;
        this.target = target;
        this.time = time;
        this.features = features;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public double[] getFeatures() {
        return features;
    }

    public void setFeatures(double[] features) {
        this.features = features;
    }

    public String toString() {
        String s = "";
        s += "(time=" + time;
        s += "; features=[" + Arrays.asList(features);
        s += "])";
        return s;
    }
    
    public String featuresToString(String separator) {
        String s = "";
        for (int i = 0; i < features.length; i++) {
            if (i > 0) {
                s += separator;
            }
            s += features[i];
        }
        return s;
    }
}
