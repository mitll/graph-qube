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

package mitll.xdata.dataset.kiva.ingest;

import org.apache.commons.io.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KivaIngest {
    public static Map<String, String> TYPE_TO_DB = new HashMap<String, String>();
    static {
        TYPE_TO_DB.put("INTEGER", "INT");
        TYPE_TO_DB.put("STRING", "VARCHAR");
        TYPE_TO_DB.put("DATE", "TIMESTAMP");
        TYPE_TO_DB.put("REAL", "DOUBLE");
        TYPE_TO_DB.put("BOOLEAN", "BOOLEAN");
    }

/*    public static Object[] getColumnSpecs(String schema) {
        List<String> names = new ArrayList<String>();
        List<String> types = new ArrayList<String>();
        String[] columns = schema.split(";");
        for (int i = 0; i < columns.length; i++) {
            int index = columns[i].indexOf(":");
            names.add(columns[i].substring(0, index).trim());
            types.add(columns[i].substring(index + 1).trim());
        }
        return new Object[] { names, types };
    }*/

    public static Object[] processSchema(String filename) throws Exception {
        // teams:category: [String]
        // teams:image:id: [Integer]

        String schema = FileUtils.readFileToString(new File(filename), "UTF-8").trim();

        List<String> names = new ArrayList<String>();
        List<String> types = new ArrayList<String>();

        String regex = "([a-zA-Z_0-9:\\[\\]]+):\\s+\\[([a-zA-Z_0-9]+)\\]";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(schema);

        while (matcher.find()) {
            String type = matcher.group(2).trim();
            String name = matcher.group(1).trim();

            // replace empty [] with nothing
            name = name.replaceAll("\\[\\]", "");

            // strip off final colons
            name = name.replaceAll(":+$", "");

            // replace internal ":" with "_"
            name = name.replaceAll(":+", "_");

            names.add(name);
            types.add(type);
        }

        return new Object[] { names, types };
    }

    public static String createCreateSQL(String tableName, List<String> names, List<String> types) {
        String sql = "CREATE TABLE " + tableName + " (" + "\n";
        for (int i = 0; i < names.size(); i++) {
            sql += (i > 0 ? ",\n  " : "  ") + names.get(i) + " " + TYPE_TO_DB.get(types.get(i).toUpperCase());
        }
        sql += "\n);";
        return sql;
    }

    public static String createInsertSQL(String tableName, List<String> names) {
        String sql = "INSERT INTO " + tableName + " (";
        for (int i = 0; i < names.size(); i++) {
            sql += (i > 0 ? ", " : "") + names.get(i);
        }
        sql += ") VALUES (";
        for (int i = 0; i < names.size(); i++) {
            sql += (i > 0 ? ", " : "") + "?";
        }
        sql += ");";
        return sql;
    }

    public static int executePreparedStatement(PreparedStatement statement, List<String> types, List<String> values)
            throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z");
        try {
            for (int i = 0; i < types.size(); i++) {
                String type = TYPE_TO_DB.get(types.get(i).toUpperCase());
                String value = values.get(i);
                if (value != null && value.trim().length() == 0) {
                    value = null;
                }
                if (type.equalsIgnoreCase("INT")) {
                    if (value == null) {
                        statement.setNull(i + 1, java.sql.Types.INTEGER);
                    } else {
                        statement.setInt(i + 1, Integer.parseInt(value, 10));
                    }
                } else if (type.equalsIgnoreCase("DOUBLE")) {
                    if (value == null) {
                        statement.setNull(i + 1, java.sql.Types.DOUBLE);
                    } else {
                        statement.setDouble(i + 1, Double.parseDouble(value));
                    }
                } else if (type.equalsIgnoreCase("BOOLEAN")) {
                    if (value == null) {
                        statement.setNull(i + 1, java.sql.Types.BOOLEAN);
                    } else {
                        statement.setBoolean(i + 1, Boolean.parseBoolean(value));
                    }
                } else if (type.equalsIgnoreCase("VARCHAR")) {
                    statement.setString(i + 1, value);
                } else if (type.equalsIgnoreCase("TIMESTAMP")) {
                    if (value == null) {
                        statement.setNull(i + 1, java.sql.Types.TIMESTAMP);
                    } else {
                        statement.setTimestamp(i + 1, new Timestamp(sdf.parse(value).getTime()));
                    }
                }
            }
        } catch (Throwable e) {
            System.out.println("types = " + types);
            System.out.println("values = " + values);
            System.out.println("types.size() = " + types.size());
            System.out.println("values.size() = " + values.size());
            e.printStackTrace();
            System.out.println(e.getMessage());
            throw new Exception(e);
        }
        return statement.executeUpdate();
    }

    /**
     * Assumes at least one field.
     */
    public static List<String> split(String s, String separator) {
        List<String> fields = new ArrayList<String>();
        int i = 0;
        // add fields up to last separator
        while (i < s.length()) {
            int index = s.indexOf(separator, i);
            if (index < 0) {
                break;
            }
            fields.add(s.substring(i, index));
            i = index + 1;
        }
        // add field after last separator
        fields.add(s.substring(i, s.length()));
        return fields;
    }

    @SuppressWarnings("unchecked")
    public static void loadTable(String tableName, String schemaFilename, String dataFilename) throws Exception {
        Object[] temp = processSchema(schemaFilename);
        List<String> names = (List<String>) temp[0];
        List<String> types = (List<String>) temp[1];

        Class.forName("org.h2.Driver");
        Connection connection = DriverManager.getConnection("jdbc:h2:tcp://localhost//h2data/kiva/kiva", "sa", "");

        String createSQL = createCreateSQL(tableName, names, types);
        String insertSQL = createInsertSQL(tableName, names);

        PreparedStatement statement = connection.prepareStatement(createSQL);
        statement.executeUpdate();
        statement.close();

        statement = connection.prepareStatement(insertSQL);

        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(dataFilename), "UTF-8"));
        String line = null;
        int count = 0;
        long t0 = System.currentTimeMillis();
        while ((line = br.readLine()) != null) {
            count++;
            List<String> values = split(line, "\t");
            executePreparedStatement(statement, types, values);
            if (count % 10000 == 0) {
                System.out.println("count = " + count + "; " + (System.currentTimeMillis() - 1.0 * t0) / count
                        + " ms/insert");
            }
        }
        br.close();

        statement.close();

        long t1 = System.currentTimeMillis();
        System.out.println("total count = " + count);
        System.out.println("total time = " + ((t1 - t0) / 1000.0) + " s");
        System.out.println((t1 - 1.0 * t0) / count + " ms/insert");
        System.out.println((1000.0 * count / (t1 - 1.0 * t0)) + " inserts/s");
    }

/*    public static void testTabs(String dataFilename) throws Exception {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(dataFilename), "UTF-8"));
        String line = null;
        int count = 0;
        int firstNumTabs = -1;
        while ((line = br.readLine()) != null) {
            count++;
            int numTabs = 0;
            boolean lastTabLastCharacter = false;
            for (int i = 0; i < line.length(); i++) {
                if (line.charAt(i) == '\t') {
                    numTabs++;
                    if (i == line.length() - 1) {
                        lastTabLastCharacter = true;
                    }
                }
            }
            if (firstNumTabs == -1) {
                firstNumTabs = numTabs;
                System.out.println("firstNumTabs = " + firstNumTabs);
            }
            if (numTabs != firstNumTabs) {
                System.out.println("line " + count + ": numTabs = " + numTabs);
            }
            if (lastTabLastCharacter) {
                System.out.println("line " + count + ": lastTabLastCharacter = " + lastTabLastCharacter);
                break;
            }
            if (count % 50000 == 0) {
                System.out.println("count = " + count);
            }
        }
        br.close();
    }*/

    public static void main(String[] args) throws Exception {
        // System.out.println(split("\ta\tb\t\tc\t", "\t"));

        String tableName = "loanJournalEntriesLinks";
        String schemaFilename = "kiva_schemas/" + tableName + ".schema";
        String dataFilename = "C:/Users/user/Desktop/xdata/datasets/kiva/" + tableName + ".csv";
        loadTable(tableName, schemaFilename, dataFilename);

        System.out.println("done");
    }
}
