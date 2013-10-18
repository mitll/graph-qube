package mitll.xdata.hmm;

public interface ObservationLikelihood<T extends Observation> {
	public double likelihood(T observation);
	public double logLikelihood(T observation);
}
