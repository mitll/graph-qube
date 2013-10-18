// Copyright 2013 MIT Lincoln Laboratory, Massachusetts Institute of Technology 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mitll.xdata;

import influent.idl.FL_EntityMatchDescriptor;
import influent.idl.FL_EntityMatchResult;
import influent.idl.FL_LinkMatchDescriptor;
import influent.idl.FL_PatternDescriptor;
import influent.idl.FL_PatternSearchResult;
import influent.idl.FL_PatternSearchResults;
import influent.idl.FL_Property;
import influent.idl.FL_SingletonRange;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;

import com.itextpdf.text.log.Logger;

public class AvroUtils {
    /**
     * Encodes an Avro object as JSON string.
     * 
     * Note: Just calling record.toString() doesn't encode union types losslessly (since it leaves out their type).
     */
    public static String encodeJSON(SpecificRecordBase record) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        JsonEncoder encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), baos);
        DatumWriter<SpecificRecord> datumWriter = new SpecificDatumWriter<SpecificRecord>(record.getSchema());
        datumWriter.write(record, encoder);
        encoder.flush();
        return baos.toString();
    }

    /**
     * Decodes an Avro object from a JSON string.
     * 
     * Note: This method will fail on strings created by calling record.toString() since that doesn't encode union types
     * losslessly.
     */
    public static SpecificRecord decodeJSON(Schema schema, String json) throws Exception {
        DatumReader<SpecificRecord> datumReader = new SpecificDatumReader<SpecificRecord>(schema);
        JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema, json);
        SpecificRecord record = datumReader.read(null, decoder);
        return record;
    }

    /**
     * Creates an FL_PatternDescriptor based on set of entity IDs (not considering links).
     * 
     * @param ids
     *            global IDs (guids) for entities in query.
     * @return FL_PatternDescriptor with exemplars corresponding to IDs.
     */
    public static FL_PatternDescriptor createExemplarQuery(List<String> ids) {
        FL_PatternDescriptor patternDescriptor = new FL_PatternDescriptor();

        patternDescriptor.setUid("PD0");
        patternDescriptor.setName("Pattern Descriptor 0");
        patternDescriptor.setLinks(new ArrayList<FL_LinkMatchDescriptor>());

        List<FL_EntityMatchDescriptor> entityMatchDescriptors = new ArrayList<FL_EntityMatchDescriptor>();
        List<String> exemplars;

        for (int i = 0; i < ids.size(); i++) {
            exemplars = Arrays.asList(new String[] { ids.get(i) });
            entityMatchDescriptors.add(new FL_EntityMatchDescriptor("E" + i, "Entity Role " + i, null, null, null,
                    exemplars, null));
        }
        patternDescriptor.setEntities(entityMatchDescriptors);

        return patternDescriptor;
    }

    public static String toString(FL_PatternSearchResult patternSearchResult) {
        String s = "";
        s += patternSearchResult.getScore() + "\t";
        for (FL_EntityMatchResult entityMatchResult : patternSearchResult.getEntities()) {
            s += entityMatchResult.getUid() + ":" + entityMatchResult.getEntity().getUid() + " ("
                    + entityMatchResult.getScore() + ") " + "\t";
        }
        return s;
    }

    public static void displaySubgraphsAsTable(FL_PatternSearchResults patternSearchResults) {
        for (FL_PatternSearchResult patternSearchResult : patternSearchResults.getResults()) {
            System.out.println(toString(patternSearchResult));
        }
    }

    /**
     * Converts entities returned from node similarity search to HTML table.
     * 
     * Note: assumes there's only one FL_EntityMatchResult per FL_PatternSearchResult.
     */
    public static String entityListAsTable(FL_PatternSearchResults patternSearchResults) {
        // get all properties
        Set<String> propertyKeySet = new TreeSet<String>();
        for (FL_PatternSearchResult patternSearchResult : patternSearchResults.getResults()) {
            FL_EntityMatchResult entityMatchResult = patternSearchResult.getEntities().get(0);
            for (FL_Property property : entityMatchResult.getEntity().getProperties()) {
                propertyKeySet.add(property.getKey());
            }
        }
        List<String> propertyKeys = new ArrayList<String>(propertyKeySet);

        String html = "";
        html += "<table>";
        html += "<tr>";
        html += "<th>Score</th>";
        html += "<th>ID</th>";
        for (String name : propertyKeys) {
            html += "<th>" + name + "</th>";
        }
        html += "</tr>";
        for (FL_PatternSearchResult patternSearchResult : patternSearchResults.getResults()) {
            FL_EntityMatchResult entityMatchResult = patternSearchResult.getEntities().get(0);
            html += "<tr>";
            html += "<td>" + entityMatchResult.getScore() + "</td>";
            html += "<td>" + entityMatchResult.getEntity().getUid() + "</td>";
            for (String name : propertyKeys) {
                String value = "";
                for (FL_Property property : entityMatchResult.getEntity().getProperties()) {
                    if (property.getKey().equalsIgnoreCase(name)) {
                        Object range = property.getRange();
                        if (range instanceof FL_SingletonRange) {
                            FL_SingletonRange singleton = (FL_SingletonRange) range;
                            value = singleton.getValue() != null ? singleton.getValue().toString() : "null";
                        } else {
                            // TODO : handle non-singleton values
                        }
                    }
                }
                html += "<td>" + value + "</td>";
            }
            html += "</tr>";
        }
        html += "</table>";
        return html;
    }
}
