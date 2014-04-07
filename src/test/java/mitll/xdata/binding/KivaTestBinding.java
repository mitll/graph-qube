package mitll.xdata.binding;

import influent.idl.FL_EntityMatchDescriptor;
import influent.idl.FL_LinkMatchDescriptor;
import influent.idl.FL_PatternDescriptor;
import influent.idl.FL_PatternSearchResults;
import mitll.xdata.AvroUtils;
import mitll.xdata.dataset.kiva.binding.KivaBinding;
import mitll.xdata.db.DBConnection;
import mitll.xdata.db.H2Connection;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Created by go22670 on 4/7/14.
 */
public class KivaTestBinding {
  private static Logger logger = Logger.getLogger(KivaTestBinding.class);


  public static void main(String[] args) throws Exception {
    System.getProperties().put("logging.properties", "log4j.properties");
    // System.getProperties().put("log4j.configuration", "/log4j.properties");

    KivaBinding kivaBinding = null;

    DBConnection connection;
//        if (args.length == 3) {
//            // database, user, password
//            connection = new MysqlConnection(args[0], args[1], args[2]);
//        } else {
//            connection = new MysqlConnection("kiva");
//        }
    connection = new H2Connection("c:/temp/kiva", "kiva");
    try {
      kivaBinding = new KivaBinding(connection);
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }
    logger.debug("got " + kivaBinding);
    System.out.println("got " + kivaBinding);

    FL_PatternDescriptor query = new FL_PatternDescriptor();
    query.setUid("PD1");
    query.setName("Pattern Descriptor 1");
    query.setLinks(new ArrayList<FL_LinkMatchDescriptor>());

    List<String> exemplars;
    List<FL_EntityMatchDescriptor> entityMatchDescriptors = new ArrayList<FL_EntityMatchDescriptor>();
    exemplars = Arrays.asList(new String[]{"l0376099"});
    entityMatchDescriptors.add(new FL_EntityMatchDescriptor("L1", "Unlucky Lender", null, null, null, exemplars, null));
    exemplars = Arrays.asList(new String[] { "p137" });
    entityMatchDescriptors.add(new FL_EntityMatchDescriptor("P1", "Risky Partner", null, null, null, exemplars, null));
    query.setEntities(entityMatchDescriptors);

    System.out.println("lender and partner query:");
    System.out.println(AvroUtils.encodeJSON(query));

    // http://localhost:4567/pattern/search/example?example={"uid":"PD1","name":"Pattern Descriptor 1","description":null,"entities":[{"uid":"L1","role":{"string":"Unlucky Lender"},"sameAs":null,"entities":null,"tags":null,"properties":null,"examplars":{"array":["l0376099"]},"weight":1.0},{"uid":"P1","role":{"string":"Risky Partner"},"sameAs":null,"entities":null,"tags":null,"properties":null,"examplars":{"array":["p137"]},"weight":1.0}],"links":[]}&max=20
    Object result = kivaBinding.searchByExample(query, "", 0, 4);
    AvroUtils.displaySubgraphsAsTable((FL_PatternSearchResults) result);
    System.out.println("result = " + AvroUtils.encodeJSON((FL_PatternSearchResults) result));

    entityMatchDescriptors.clear();
    exemplars = Arrays.asList(new String[] { "l0376099" });
    entityMatchDescriptors.add(new FL_EntityMatchDescriptor("L1", "Unlucky Lender", null, null, null, exemplars, null));
    query.setEntities(entityMatchDescriptors);
    System.out.println("just lender query:");
    System.out.println(AvroUtils.encodeJSON(query));

    result = kivaBinding.searchByExample(query, "", 0, 4);
    System.out.println("result = " + result);

    entityMatchDescriptors.clear();
    exemplars = Arrays.asList(new String[] { "p137" });
    entityMatchDescriptors.add(new FL_EntityMatchDescriptor("P1", "Risky Partner", null, null, null, exemplars, null));
    query.setEntities(entityMatchDescriptors);
    System.out.println("just partner query:");
    System.out.println(AvroUtils.encodeJSON(query));
    result = kivaBinding.searchByExample(query, "", 0, 4);

    System.out.println("result = " + result);

    // FL_PatternDescriptor descriptor = AvroUtils.createExemplarQuery(Arrays.asList(new String[] {"lmarie8422",
    // "lmike1401", "p137"}));
    // FL_PatternDescriptor descriptor = AvroUtils.createExemplarQuery(Arrays.asList(new String[] { "lmarie8422",
    // "lmike1401", "lgooddogg1", "p137" }));
    FL_PatternDescriptor descriptor = AvroUtils.createExemplarQuery(Arrays.asList(new String[] { "lmarie8422",
        "lmike1401", "lgooddogg1", "ltrolltech4460", "p137", "p65" }));
    logger.debug("descriptor = " + AvroUtils.encodeJSON(descriptor));
    result = kivaBinding.searchByExample(descriptor, null, 0, 10);
    AvroUtils.displaySubgraphsAsTable((FL_PatternSearchResults) result);
    // System.out.println("result = " + result);
    // System.out.println("result = " + AvroUtils.encodeJSON((FL_PatternSearchResults) result));

    if (true) {
      return;
    }

    logger.debug("--------------------------------");

    try {

      // loans for a lender
      // Graph loansForSkylar = kivaBinding.getOneHopGraph(LENDERS, "skylar", LOAN_META_DATA, 400);
      // logger.debug("got " + loansForSkylar.print());

      // teams for a lender
      // Graph teamForSkylar = kivaBinding.getOneHopGraph(LENDERS, "skylar", TEAMS, 400);
      // logger.debug("got " + teamForSkylar.print());

      // lenders for a team
      // Graph lendersForTeam = kivaBinding.getOneHopGraph(TEAMS, "2598", LENDERS, 400);
      // logger.debug("got " + lendersForTeam.print());

      // loans for a partner
      // Graph loanForPartner = kivaBinding.getOneHopGraph(PARTNERS_META_DATA, "22", LOAN_META_DATA, 20);
      // logger.debug("got " + loanForPartner.print() + "\npartner "
      //         + loanForPartner.getNodes().iterator().next().props);
      // if (true) return;

      logger.debug("--------------------------------");

      // Map<String, String> entity = kivaBinding.getEntity("t" + "5048");
      // Collection<ResultInfo> entities = kivaBinding.getEntitiesForTag(FL_PropertyTag.ID, "5048", 20);
      // Collection<ResultInfo> entities = kivaBinding.getEntitiesForTag(FL_PropertyTag.LABEL, "skylar", 20);

//            FL_PropertyMatchDescriptor p1 = FL_PropertyMatchDescriptor.newBuilder()
//                    .setKey(FL_PropertyTag.LABEL.toString()).setConstraint(FL_Constraint.EQUALS).setValue("skylar")
//                    .build();
//            FL_PropertyMatchDescriptor p2 = FL_PropertyMatchDescriptor.newBuilder()
//                    .setKey(FL_PropertyTag.DATE.toString()).setConstraint(FL_Constraint.GREATER_THAN)
//                    .setValue("2012-04-01 01:01:01").build();
//
//            FL_PropertyMatchDescriptor p3 = FL_PropertyMatchDescriptor.newBuilder()
//                    .setKey(FL_PropertyTag.LABEL.toString()).setConstraint(FL_Constraint.FUZZY).setValue("Africa")
//                    .build();
//            Collection<ResultInfo> entities = kivaBinding.getEntitiesMatchingProperties(Arrays.asList(p3, p2), 10);
//            logger.debug("got " + entities.size() + " : ");
//            for (ResultInfo r : entities)
//                logger.debug("\t" + r);

      if (true)
        return;

            /*
             * kivaBinding.getEntity("l" +"skylar"); kivaBinding.getEntity("p" +"2"); kivaBinding.getEntity("t"
             * +"5048"); kivaBinding.getEntitiesByID(LENDERS, Arrays.asList("skylar", "METS"));
             */
      // ResultInfo whereabouts = kivaBinding.getEntitiesByID(LENDERS, "whereabouts", "San Francisco CA", 3);
      // Collection<ResultInfo> whereabouts = kivaBinding.getEntitiesByID("whereabouts", "San Francisco CA", 3);
      Collection<Binding.ResultInfo> whereabouts4 = kivaBinding.getEntities(
          Arrays.asList(new Binding.Triple("whereabouts", "Francisco", "like")), 3);
      logger.debug("whereabouts " + whereabouts4);
      // if (true) return;
      Collection<Binding.ResultInfo> whereabouts3 = kivaBinding
          .getEntities(Arrays.asList(new Binding.Triple("whereabouts", "San Francisco"), new Binding.Triple("category",
              "Businesses")), 10);
      logger.debug("whereabouts " + whereabouts3);

      // if (true) return;
            /*
             * kivaBinding.getEntity(LOAN_META_DATA, "" + 84);
             */

      // kivaBinding.getBorrowersForLoan(""+511042);
//            FL_EntityMatchDescriptor lskylar = FL_EntityMatchDescriptor.newBuilder().setUid("123").setRole("")
//                    .setSameAs("").setEntities(Arrays.asList("lskylar")).build();
//
//            FL_PropertyMatchDescriptor whereabouts1 = FL_PropertyMatchDescriptor.newBuilder().setKey("whereabouts")
//                    .setConstraint(FL_Constraint.EQUALS).setValue("San Francisco").build();
//
//            FL_PropertyMatchDescriptor business = FL_PropertyMatchDescriptor.newBuilder().setKey("category")
//                    .setConstraint(FL_Constraint.EQUALS).setValue("Businesses").build();
//
//            FL_EntityMatchDescriptor whereaboutTest = FL_EntityMatchDescriptor.newBuilder().setUid("123").setRole("")
//                    .setSameAs("").setEntities(new ArrayList<String>())
//                    .setProperties(Arrays.asList(whereabouts1, business)).build();
//
//            System.out.println("descriptor : " + whereaboutTest);
//
//            FL_PatternDescriptor patternDescriptor = FL_PatternDescriptor.newBuilder().setUid("123").setName("")
//                    .setDescription("").setEntities(Arrays.asList(whereaboutTest))
//                    .setLinks(new ArrayList<FL_LinkMatchDescriptor>()).build();
//
//            FL_EntityMatchDescriptor entityMatchDescriptor = FL_EntityMatchDescriptor.newBuilder().setUid("123")
//                    .setRole("").setSameAs("").setEntities(new ArrayList<String>())
//                    .setProperties(Arrays.asList(p3, p2)).build();
//            FL_PatternDescriptor patternDescriptor2 = FL_PatternDescriptor.newBuilder().setUid("123").setName("")
//                    .setDescription("").setEntities(Arrays.asList(entityMatchDescriptor))
//                    .setLinks(new ArrayList<FL_LinkMatchDescriptor>()).build();
//
//            Object searchResult = kivaBinding.searchByExample(patternDescriptor2, "", 0, 20);
//            logger.debug("Result " + searchResult);
    } catch (Exception e) {
      e.printStackTrace(); // To change body of catch statement use File | Settings | File Templates.
    }
  }
}
