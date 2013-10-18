package mitll.xdata.scoring;

import java.util.Arrays;
import java.util.Random;

import org.apache.log4j.Logger;

public class FeatureNormalizer {
    private static Logger logger = Logger.getLogger(FeatureNormalizer.class);

    private double[][] rawFeats;
    private double[][] stdFeats;

    private int numSamples;
    private int numDims;

    private double lowerPctl;
    private double higherPctl;

    // transform for to map percentiles to [-2,2]
    private double[][] M = { { 0.5, 0.5 }, { -0.25, 0.25 } };

    /**
     * @param rawFeats
     *            input features
     * @param lowerPctl
     *            output features
     */
    public FeatureNormalizer(double[][] rawFeats, double lowerPctl, double higherPctl) {
        this.rawFeats = rawFeats;

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
    private double percentile(double[] arr, double percent) {

        double k = (numSamples - 1) * percent;
        double f = Math.floor(k);
        double c = Math.ceil(k);

        if (f == c) {
            return arr[(int) k];
        }

        double d0 = arr[(int) f] * (c - k);
        double d1 = arr[(int) c] * (k - f);
        return (d0 + d1);

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
     * @return New array with standardized features
     */
    public double[][] normalizeFeatures(double[][] rawFeats) {
        double[][] stdFeats = new double[numSamples][numDims];

        for (int i = 0; i < numDims; ++i) {
            double[] smallArr = new double[numSamples];
            double[] percentiles = new double[2];

            // Sort array
            for (int j = 0; j < numSamples; ++j) {
                smallArr[j] = rawFeats[j][i];
            }
            Arrays.sort(smallArr);

            // Get percentiles
            percentiles[0] = percentile(smallArr, lowerPctl);
            percentiles[1] = percentile(smallArr, higherPctl);

            // Get transform
            double[] b = getTransform(percentiles);

            // Actually transform
            for (int j = 0; j < numSamples; ++j) {
                stdFeats[j][i] = (rawFeats[j][i] - b[0]) / b[1];
            }
        }

        return stdFeats;
    }

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
