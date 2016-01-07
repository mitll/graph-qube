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

package mitll.xdata.dataset.bitcoin.ingest;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by go22670 on 8/6/15.
 */
public class RateConverter {
  private static final Logger logger = Logger.getLogger(RateConverter.class);

  private SortedMap<Long, Double> btcToDollar;
  private long firstDate;
  // long earliest;
  private double first;
  private long lastDate;
  // long latest;
  private double last;

  public RateConverter(String btcToDollarFile) throws Exception {
    btcToDollar = getBTCToDollar(btcToDollarFile);
    firstDate = btcToDollar.firstKey();
    // earliest = Timestamp.valueOf(firstDate + " 00:00:00").getTime();
    first = btcToDollar.get(firstDate);
    lastDate = btcToDollar.lastKey();
    // latest = Timestamp.valueOf(lastDate + " 00:00:00").getTime();
    last = btcToDollar.get(lastDate);
  }

  private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
  private final Map<String, Long> dayToTime = new HashMap<String, Long>();

  public Double getConversionRate(
      String day, long time) throws Exception {
    if (dayToTime.containsKey(day)) return getConversionRate(dayToTime.get(day), time);
    else {
      Date parse = sdf.parse(day);
      dayToTime.put(day, parse.getTime());
      return getConversionRate(parse.getTime(), time);
    }
  }

  private Double getConversionRate(
      long day, long time) {
    Double rate = btcToDollar.get(day);
    if (rate == null) {
      if (time < firstDate) rate = first;
      else if (time > lastDate) rate = last;
    }
    if (rate == null) {
      logger.warn("can't find btc->dollar rate for " + day);
      rate = 0d;
    }
    return rate;
  }

  private SortedMap<Long, Double> getBTCToDollar(String btcToDollarFile) throws Exception {
    BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(btcToDollarFile), "UTF-8"));
    String line;
    int count = 0;
    long t0 = System.currentTimeMillis();
    int max = Integer.MAX_VALUE;
    int bad = 0;
    SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
    SortedMap<Long, Double> timeToRate = new TreeMap<Long, Double>();
    while ((line = br.readLine()) != null) {
      count++;
      if (count > max) break;
      String[] split = line.split("\\s+"); //  2013-01-27   9.91897304
      if (split.length != 2) {
        bad++;
        if (bad < 10) logger.warn("badly formed line " + line);
      }
      String s = split[0];
      Date parse = sdf.parse(s);
      timeToRate.put(parse.getTime(), Double.parseDouble(split[1]));
    }

    if (bad > 0) logger.warn("Got " + bad + " transactions...");
    br.close();
    return timeToRate;
  }
}
