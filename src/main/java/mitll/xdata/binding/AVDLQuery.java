package mitll.xdata.binding;

import influent.idl.FL_EntityMatchDescriptor;
import influent.idl.FL_PatternSearchResults;
import influent.idl.FL_PropertyMatchDescriptor;
import influent.idl.FL_SearchResults;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: go22670
 * Date: 7/11/13
 * Time: 12:39 PM
 * To change this template use File | Settings | File Templates.
 */
public interface AVDLQuery {
  FL_SearchResults getSimpleSearchResult(List<FL_PropertyMatchDescriptor> properties, long max);

  FL_PatternSearchResults getSearchResult(FL_EntityMatchDescriptor descriptor, List<FL_PropertyMatchDescriptor> properties, long max);

  FL_SearchResults simpleSearch(List<FL_PropertyMatchDescriptor> properties, long start, long max);
}
