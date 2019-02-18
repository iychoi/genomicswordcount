/*
 * Copyright 2018 iychoi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package genomicswordcount.mercount;

import genomicswordcount.common.hadoop.io.format.sequence.SequenceFileInputFormat;
import genomicswordcount.common.helpers.FileSystemHelper;
import genomicswordcount.common.report.Report;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author iychoi
 */
public class MerCountMR {
    private static final Log LOG = LogFactory.getLog(MerCountMR.class);
    
    public MerCountMR() {
        
    }
    
    private void validateMerCountConfig(MerCountConfig mcConfig) throws MerCountConfigException {
        if(mcConfig.getSamplePath().size() <= 0) {
            throw new MerCountConfigException("cannot find input sample path");
        }
        
        if(mcConfig.getKmerSize() <= 0) {
            throw new MerCountConfigException("invalid kmer size");
        }
    }
    
    public int runJob(Configuration conf, MerCountConfig mcConfig) throws Exception {
        // check config
        validateMerCountConfig(mcConfig);
        
        Job job = Job.getInstance(conf, "MerCount");
        conf = job.getConfiguration();
        
        // set user configuration
        mcConfig.saveTo(conf);
        
        Report report = new Report();
        
        job.setJarByClass(MerCountMR.class);

        // Mapper
        job.setMapperClass(MerCountMRMapper.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        
        // Combiner
        job.setCombinerClass(MerCountMRCombiner.class);
        
        // Reducer
        job.setReducerClass(MerCountMRReducer.class);
        
        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        
        // Inputs
        Path[] inputSamples = FileSystemHelper.getAllSamplePaths(conf, mcConfig.getSamplePath());
        FileInputFormat.addInputPaths(job, FileSystemHelper.makeCommaSeparated(inputSamples));
        
        LOG.info("Input sample files : " + inputSamples.length);
        for(Path inputFile : inputSamples) {
            LOG.info("> " + inputFile.toString());
        }
        
        // Output
        job.setOutputFormatClass(FileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(mcConfig.getOutputPath()));
        
        // reducers
        int reducers = conf.getInt("mapred.reduce.tasks", 1);
        if(mcConfig.getTaskNum() > 0) {
            reducers = mcConfig.getTaskNum();
        }
        
        job.setNumReduceTasks(reducers);
        LOG.info("# of Reducers : " + reducers);
        
        
        // Execute job and return status
        boolean result = job.waitForCompletion(true);

        report.addJob(job);
        
        // report
        if(mcConfig.getReportPath() != null && !mcConfig.getReportPath().isEmpty()) {
            report.writeTo(mcConfig.getReportPath());
        }
        
        return result ? 0 : 1;
    }
}
