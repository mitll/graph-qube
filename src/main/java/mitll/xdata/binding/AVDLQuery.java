/*
 * Copyright 2013-2016 MIT Lincoln Laboratory, Massachusetts Institute of Technology
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
