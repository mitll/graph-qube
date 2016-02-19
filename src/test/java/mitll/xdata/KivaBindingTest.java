package mitll.xdata;

import influent.idl.FL_EntityMatchResult;
import influent.idl.FL_PatternDescriptor;
import influent.idl.FL_PatternSearchResults;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import mitll.xdata.dataset.kiva.binding.KivaBinding;
import mitll.xdata.db.DBConnection;
import mitll.xdata.db.MysqlConnection;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class KivaBindingTest {
    private static Logger logger = Logger.getLogger(KivaBindingTest.class);
    
    private static KivaBinding binding;
    
    @BeforeClass
    public static void setup() throws Exception {
        DBConnection dbConnection = new MysqlConnection("kiva", "root", "pass");
        binding = new KivaBinding(dbConnection);
    }
    
    @AfterClass
    public static void cleanup() {
    }
    
    public static void verifyDescending(List<Double> list, double tolerance) {
        for (int i = 0; i < list.size() - 1; i++) {
            Assert.assertTrue(list.get(i) - list.get(i + 1) >= -1.0 * tolerance);
        }
    }
    
    @Test
    public void testPartnerSearch() {
        // VisionFund Indonesia
        String partnerID = "p189";
        String[] expectedIDs = {"p189", "p4", "p187", "p170", "p222", "p184", "p247", "p231", "p151", "p178"};
        int max = expectedIDs.length;
        
        // perform search by example
        FL_PatternDescriptor patternDescriptor = AvroUtils.createExemplarQuery(Arrays.asList(new String[] {partnerID}));
        logger.debug("descriptor = " + patternDescriptor);
        Object object = binding.searchByExample(patternDescriptor, null, 0, max, true);
        
        // got back result
        Assert.assertTrue(object instanceof FL_PatternSearchResults);
        FL_PatternSearchResults results = (FL_PatternSearchResults) object;
        
        // number of results
        Assert.assertEquals(max, (long) results.getTotal());
        
        // IDs match
        for (int i = 0; i < results.getTotal(); i++) {
            FL_EntityMatchResult entityMatchResult = results.getResults().get(i).getEntities().get(0);
            String actualID = entityMatchResult.getEntity().getUid();
            Assert.assertEquals(expectedIDs[i], actualID);
        }
        
        // entity scores in descending order
        List<Double> scores = new ArrayList<Double>();
        for (int i = 0; i < results.getTotal(); i++) {
            FL_EntityMatchResult entityMatchResult = results.getResults().get(i).getEntities().get(0);
            scores.add(entityMatchResult.getScore());
        }
        logger.debug("entity scores = " + scores);
        verifyDescending(scores, 1e-6);
    }
    
    @Test
    public void testMultiNodeSearch() {
        String lenderID = "l0376099";
        String partnerID = "p137";
        int max = 10;

        // perform search by example
        FL_PatternDescriptor patternDescriptor = AvroUtils.createExemplarQuery(Arrays.asList(new String[] {lenderID, partnerID}));
        logger.debug("descriptor = " + patternDescriptor);
        Object object = binding.searchByExample(patternDescriptor, null, 0, max, true);
        
        // got back result
        Assert.assertTrue(object instanceof FL_PatternSearchResults);
        FL_PatternSearchResults results = (FL_PatternSearchResults) object;
        
        // number of results
        Assert.assertEquals(max, (long) results.getTotal());

        // pattern scores in descending order
        List<Double> scores = new ArrayList<Double>();
        for (int i = 0; i < results.getTotal(); i++) {
            scores.add(results.getResults().get(i).getScore());
        }
        logger.debug("pattern scores = " + scores);
        verifyDescending(scores, 1e-6);
    }
}
