package mitll.xdata.scoring;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

/**
 * Do percentile-based normalization.
 * Less sensitive to outliers.
 *
 * @see mitll.xdata.binding.Binding#getStandardFeatures(int, int, double[][])
 */
public class FeatureNormalizer {
	private static Logger logger = Logger.getLogger(FeatureNormalizer.class);

	private int numSamples;
	private int numDims;

	private double lowerPctl;
	private double higherPctl;

	// transform for to map percentiles to [-2,2]
	private double[][] M = { { 0.5, 0.5 }, { -0.25, 0.25 } };

	private boolean ignoreZeros = false;


	/**
	 * @param rawFeats
	 *            input features
	 * @param lowerPctl
	 *            output features
	 * @see mitll.xdata.binding.Binding#getStandardFeatures(int, int, double[][])
	 */
	public FeatureNormalizer(double[][] rawFeats, double lowerPctl, double higherPctl,boolean ignoreZeros) {
		this(rawFeats, lowerPctl, higherPctl);

		this.ignoreZeros = ignoreZeros;
	}

	/**
	 * @param rawFeats
	 *            input features
	 * @param lowerPctl
	 *            output features
	 * @see mitll.xdata.binding.Binding#getStandardFeatures(int, int, double[][])
	 */
	public FeatureNormalizer(double[][] rawFeats, double lowerPctl, double higherPctl) {
		this.numSamples = rawFeats.length;
		this.numDims = rawFeats[0].length;

		this.lowerPctl = lowerPctl;
		this.higherPctl = higherPctl;
	}

	/**
	 * @param arr
	 *            SORTED array of values
	 * @param percent
	 *            percentile desired
	 * 
	 * @return Return percentile for input data
	 */
	private double percentile(Double[] arr, double percent) {
		int numExemplars = arr.length;

		if (numExemplars > 0) {

			double k = (numExemplars - 1) * percent;
			double f = Math.floor(k);
			double c = Math.ceil(k);

			if (f == c) {
				return arr[(int) k];
			}

			logger.info("numExemplars: "+numExemplars+" k: "+k+" f: "+f+" c: "+c);

			double d0 = arr[(int) f] * (c - k);
			double d1 = arr[(int) c] * (k - f);
			return (d0 + d1);

		} else {
			return -1.0;
		}
	}

	/**
	 * @param percentiles
	 *            Percentiles
	 * 
	 * @return Return transform
	 */
	private double[] getTransform(double[] percentiles) {
		double[] b = new double[2];

		b[0] = M[0][0] * percentiles[0] + M[0][1] * percentiles[1];
		b[1] = M[1][0] * percentiles[0] + M[1][1] * percentiles[1];

		return b;
	}


	/**
	 * @see mitll.xdata.dataset.bitcoin.features.BitcoinFeatures#getStandardizedFeatures(java.util.List)
	 * @return New array with standardized features
	 */
	public double[][] normalizeFeatures(double[][] rawFeats) {
		double[][] stdFeats = new double[numSamples][numDims];

		for (int i = 0; i < numDims; ++i) {
			//			double[] smallArr = new double[numSamples];
			double[] percentiles = new double[2];

			//			for (int j = 0; j < numSamples; ++j) {
			//				smallArr[j] = rawFeats[j][i];
			//			}
			Double[] smallArr = computeSmallArr(rawFeats, i);

			// Sort array
			Arrays.sort(smallArr);

			if (smallArr.length > 0) {

				// Get percentiles
				percentiles[0] = percentile(smallArr, lowerPctl);
				percentiles[1] = percentile(smallArr, higherPctl);

				// Get transform
				double[] b = getTransform(percentiles);

				// Actually transform
				for (int j = 0; j < numSamples; ++j) {
					if (ignoreZeros & (rawFeats[j][i] == 0.0)) {
						stdFeats[j][i] = 0.0;
					} else {
						stdFeats[j][i] = (rawFeats[j][i] - b[0]) / b[1];
					}
				}
				logger.info("b[0]: "+b[0]+" b[1]: "+b[1]);

			} else {
				for (int j = 0; j < numSamples; ++j) {
					stdFeats[j][i] = 0.0;
				}
				logger.info("no actual data...");
			}
		}

		return stdFeats;
	}


	/**
	 * Compute the feature exemplars to use for standardization
	 */
	private Double[] computeSmallArr(double[][] rawFeats, int dimInd) {
		ArrayList<Double> smallArr = new ArrayList<Double>();
		int numSamples = rawFeats.length;
		if (ignoreZeros) {
			// get only non-zero features
			for (int i=0;i<numSamples;i++) {
				if (rawFeats[i][dimInd] != 0.0) {
					smallArr.add(rawFeats[i][dimInd]);
				} //else {
					//logger.info("zero element found: "+rawFeats[i][dimInd]);
				//}
			}
		} else {
			
			for (int i=0;i<numSamples;i++) {
				smallArr.add(rawFeats[i][dimInd]);
			}
		}

		return smallArr.toArray(new Double[smallArr.size()]);
	}
	

	/**
	 * Just for testing.
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// Percentiles to standardize by
		double lowerPctl = 0.025;
		double higherPctl = 0.975;

		// Initialize sample array
		int row = 10;
		int col = 3;

		double[][] testArray = new double[row][col];

		// Random matrix for testing..
		Random generator = new Random();
		for (int i = 0; i < row; ++i) {
			for (int j = 0; j < col; ++j) {
				testArray[i][j] = generator.nextDouble();
			}
		}

		testArray = new double[][] {
				{1, 101, 1001},
				{2, 102, 1002},
				{3, 103, 1003},
				{4, 104, 1004},
				{5, 105, 1005},
				{6, 106, 1006}
		};

		// System.out.println("================================================");
		// System.out.println(Arrays.deepToString(testArray));
		// Initialize normalizer
		FeatureNormalizer standardizer = new FeatureNormalizer(testArray, lowerPctl, higherPctl);

		double[][] stdFeats = standardizer.normalizeFeatures(testArray);

		System.out.println("================================================");
		System.out.println(Arrays.deepToString(testArray));
		System.out.println("================================================");
		System.out.println(Arrays.deepToString(stdFeats));

	}
}
