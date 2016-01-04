package mitll.xdata.hmm;

import org.apache.log4j.Logger;

import java.util.List;

/**
 * @see mitll.xdata.binding.Binding#makeHMM(java.util.List)
 */
public class KernelDensityLikelihood implements ObservationLikelihood<VectorObservation> {
	private static Logger logger = Logger.getLogger(KernelDensityLikelihood.class);

	private List<VectorObservation> observations;

	private double bandwidth;

  /**
   * @see mitll.xdata.binding.Binding#makeHMM(java.util.List)
   * @param observations
   * @param bandwidth
   */
	public KernelDensityLikelihood(List<VectorObservation> observations, double bandwidth) {
		this.observations = observations;
		this.bandwidth = bandwidth;
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
   * @see #logLikelihood(VectorObservation)
   * @see mitll.xdata.hmm.Hmm#decodeTopK(java.util.List, int)
   * @see mitll.xdata.hmm.Hmm#probability(java.util.List, java.util.List)
	 */
	@Override
	public double likelihood(VectorObservation x) {
		int N = observations.size();
		int p = observations.get(0).getValues().length;
		double sum = 0.0;
		for (int i = 0; i < N; i++) {
			VectorObservation xi = observations.get(i);
			double d = distance(xi.getValues(), x.getValues());
			sum += Math.exp(-0.5 * Math.pow(d / bandwidth, 2.0));
		}
		double prob = sum / Math.pow(N * 2 * bandwidth * bandwidth * Math.PI, p / 2.0);
		// have a "floor" (even though this makes it an improper distribution)?
		double floor = 1e-18;
		if (prob >= 0.0) {
			// logger.debug("(prob <= floor) = " + (prob <= floor));
			return Math.max(prob, floor);
		} else {
			return floor;
		}

	}

	@Override
	public double logLikelihood(VectorObservation observation) {
		return Math.log(likelihood(observation));
	}
}
