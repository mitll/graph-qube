package mitll.xdata.hmm;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class KernelDensityLikelihoodTest {
	private static Logger logger = Logger.getLogger(KernelDensityLikelihoodTest.class);

	public static VectorObservation observation(double... data) {
		double[] values = new double[data.length];
		for (int i = 0; i < data.length; i++) {
			values[i] = data[i];
		}
		return new VectorObservation(values);
	}
	
	@Test
	public void testKernelDensity() {
		List<VectorObservation> observations = new ArrayList<VectorObservation>();
		observations.add(observation(2.0, 4.0));
        KernelDensityLikelihood kd = new KernelDensityLikelihood(observations, 1.0);
        logger.debug(kd.likelihood(observation(2.0, 4.0)));
        logger.debug(kd.likelihood(observation(2.0, 6.0)));
        Assert.assertTrue(kd.likelihood(observation(2.0, 4.0)) > kd.likelihood(observation(2.0, 6.0)));
	}
}
