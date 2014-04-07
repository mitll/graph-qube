package mitll.xdata.experimental.graph;

import mitll.xdata.binding.Binding;

import java.util.List;

/**
 *
 * Created with IntelliJ IDEA.
 * User: go22670
 * Date: 7/1/13
 * Time: 3:35 PM
 * To change this template use File | Settings | File Templates.
 */
public interface GraphQuery {
  Graph getOneHopGraph(String fromTable, List<Binding.Triple> sourceEntitySearchCriteria,
                       List<Binding.Triple> linkSearchCriteria,
                       String toTable, List<Binding.Triple> targetEntitySearchCriteria,
                       long limit
  );

  Graph getSubgraphForEntitiesInTimeRange(List<Binding.Triple> entitySearchParameters,
                                          String start, String end,
                                          List<Binding.Triple> linkSearchParameters, long limit);

  List<Graph> getSubgraphsForEntitiesInTimeRanges(List<Binding.Triple> entitySearchParameters,
                                                  List<TimeRange> timeRanges,
                                                  List<Binding.Triple> linkSearchParameters, long limit);

  public static class TimeRange {
    String start, end;
    public TimeRange(String start, String end) { this.start = start; this.end = end; }
  }
}
