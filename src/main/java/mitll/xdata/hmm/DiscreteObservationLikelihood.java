package mitll.xdata.hmm;

public class DiscreteObservationLikelihood implements ObservationLikelihood<DiscreteObservation> {
	/** probability mass function over values 0, ..., N-1 for vocabulary of size N */
	private double[] pmf;
	
	public DiscreteObservationLikelihood(double[] pmf) {
		this.pmf = pmf;
	}
	
	@Override
	public double likelihood(DiscreteObservation obs) {
		return pmf[obs.getValue()];
	}

	@Override
	public double logLikelihood(DiscreteObservation obs) {
		return Math.log(likelihood(obs));
	}
}
