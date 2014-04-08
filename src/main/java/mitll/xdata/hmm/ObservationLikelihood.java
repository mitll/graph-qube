package mitll.xdata.hmm;

/**
 * @see mitll.xdata.hmm.Hmm#decodeTopK(java.util.List, int)
 * @param <T>
 */
public interface ObservationLikelihood<T extends Observation> {
	public double likelihood(T observation);
	public double logLikelihood(T observation);
}
