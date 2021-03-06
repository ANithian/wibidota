/* Copyright 2013 WibiData, Inc.
*
* See the NOTICE file distributed with this work for additional
* information regarding copyright ownership.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.kiji.wibidota;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * A simple class that represents a job to find all non-anonymous players
 */
public class NonAnonymousPlayers extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(NonAnonymousPlayers.class);

  /**
   * Some useful counters to keep track of while processing match history.
   */
  static enum NonAnonymousPlayersCounters {
    MALFORMED_MATCH_LINES,
    NONANONYMOUS_PLAYER_TOTAL,
    UNIQUE_NONANONYMOUS_MATCHES, // Number of anonymous matches played, not double counting for multiple players.
    NONANONYMOUS_MATCHES // Number of nonanonymous matches played, counting each player individually.
  }

  /*
   * A mapper that reads over matches from the input files and outputs <player_id, match_id>
   * for every non-anonymous player in the match
   */
  public static class Map extends Mapper<LongWritable, Text, LongWritable, IntWritable> {
    // The constant Valve uses for anonymous players. Equal to -1 in signed 32 bits.
    private final static long ANONYMOUS_ID = 4294967295L;

    private final static JsonParser PARSER = new JsonParser();
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      try{
        boolean nonAnonymousPlayerFound = false;
        JsonObject matchTree = PARSER.parse(value.toString()).getAsJsonObject();
        IntWritable matchId = new IntWritable(matchTree.get("match_id").getAsInt());
        for (JsonElement playerElem : matchTree.get("players").getAsJsonArray()) {
          JsonObject player = playerElem.getAsJsonObject();
          JsonElement account_id = player.get("account_id");
          if (null == account_id) {
            // Player is a bot.
            return;
          }
          long player_id = account_id.getAsLong();
          if (player_id != ANONYMOUS_ID) {
            nonAnonymousPlayerFound = true;
            context.write(new LongWritable(player_id), matchId);
          }
        }
        if (nonAnonymousPlayerFound) {
          context.getCounter(NonAnonymousPlayersCounters.UNIQUE_NONANONYMOUS_MATCHES).increment(1);
        }
      } catch (IllegalStateException e) {
        // Indicates malformed JSON.
        context.getCounter(NonAnonymousPlayersCounters.MALFORMED_MATCH_LINES).increment(1);
      }
    }
  }

  /**
   * A simple reducer that simply outputs the non-anonymous player accounts with their play count.
   */
  public static class Reduce
      extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
    public void reduce(LongWritable key, Iterable<IntWritable> values, Context context) 
        throws IOException, InterruptedException {
      context.getCounter(NonAnonymousPlayersCounters.NONANONYMOUS_PLAYER_TOTAL).increment(1);
      int total = 0;
      for (IntWritable match : values) {
        total++;
      }
      context.getCounter(NonAnonymousPlayersCounters.NONANONYMOUS_MATCHES).increment(total);
      context.write(key, new IntWritable(total));
    }
  }

  public static void main(String args[]) throws Exception {
    Configuration conf = new Configuration();
    int res = ToolRunner.run(conf, new NonAnonymousPlayers(), args);
    System.exit(res);
  }

  public final int run(final String[] args) throws Exception {
    Job job = new Job(super.getConf(), "non-anonymous");
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    if (job.waitForCompletion(true)) {
      return 0;
    } else {
      return -1;
    }
  }
}

