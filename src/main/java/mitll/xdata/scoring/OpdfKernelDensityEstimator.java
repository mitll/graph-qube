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

import be.ac.ulg.montefiore.run.jahmm.ObservationVector;
import be.ac.ulg.montefiore.run.jahmm.Opdf;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class OpdfKernelDensityEstimator implements Opdf<ObservationVector> {
    private static final long serialVersionUID = 1L;

    private List<ObservationVector> observations;
    
    private double bandwidth;
    
    public OpdfKernelDensityEstimator(List<ObservationVector> observations, double bandwidth) {
        this.observations = observations;
        this.bandwidth = bandwidth;
    }
    
    public Opdf<ObservationVector> clone() {
        return new OpdfKernelDensityEstimator(new ArrayList<ObservationVector>(observations), bandwidth);
    }

    @Override
    public void fit(ObservationVector... arg0) {
        throw new RuntimeException("fit() not supported");
    }

    @Override
    public void fit(Collection<? extends ObservationVector> arg0) {
        throw new RuntimeException("fit() not supported");
    }

    @Override
    public void fit(ObservationVector[] arg0, double[] arg1) {
        throw new RuntimeException("fit() not supported");
    }

    @Override
    public void fit(Collection<? extends ObservationVector> arg0, double[] arg1) {
        throw new RuntimeException("fit() not supported");
    }

    @Override
    public ObservationVector generate() {
        throw new RuntimeException("generate() not supported");
    }

    private double distance(double[] x0, double[] x1) {
        double d = 0.0;
        for (int i = 0; i < x0.length; i++) {
            double diff = x0[i] - x1[i];
            d += diff * diff;
        }
        d = Math.sqrt(d);
        return d;
    }
    
    /**
     * Computes probability of observation using kernel density estimate with Gaussian kernel.
     * 
     * See "The Elements of Statistical Learning" (Hastie et al 2009), page 209.
     * 
     * @see be.ac.ulg.montefiore.run.jahmm.Opdf#probability(be.ac.ulg.montefiore.run.jahmm.Observation)
     */
    @Override
    public double probability(ObservationVector x) {
        int N = observations.size();
        int p = observations.get(0).dimension();
        double sum = 0.0;
        for (int i = 0; i < N; i++) {
            ObservationVector xi = observations.get(i);
            double d = distance(xi.values(), x.values());
            sum += Math.exp(-0.5 * Math.pow(d / bandwidth, 2.0));
        }
        double prob = sum / Math.pow(N * 2 * bandwidth * bandwidth * Math.PI, p / 2.0);
        // have a "floor" (even though this makes it an improper distribution)?
        double floor = 1e-18;
        if (prob >= 0.0) {
            return Math.max(prob, floor);
        } else {
            return floor;
        }
    }

    @Override
    public String toString(NumberFormat arg0) {
        return null;
    }
}
