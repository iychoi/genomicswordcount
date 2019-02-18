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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import genomicswordcount.common.json.JsonSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class MerCountConfig {
    
    public static final int DEFAULT_KMERSIZE = 3;
    public static final int DEFAULT_TASKNUM = 0; // use system default
    public static final String DEFAULT_OUTPUT_PATH = "mercount_output";
    
    protected static final String HADOOP_CONFIG_KEY = "genomicswordcount.mercount.mercountconfig";
    
    private String reportFilePath;
    
    private int kmerSize = DEFAULT_KMERSIZE;
    private int taskNum = DEFAULT_TASKNUM;
    private List<String> samplePaths = new ArrayList<String>();
    private String outputPath = DEFAULT_OUTPUT_PATH;
    
    public static MerCountConfig createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (MerCountConfig) serializer.fromJsonFile(file, MerCountConfig.class);
    }
    
    public static MerCountConfig createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (MerCountConfig) serializer.fromJson(json, MerCountConfig.class);
    }
    
    public static MerCountConfig createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (MerCountConfig) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, MerCountConfig.class);
    }
    
    public static MerCountConfig createInstance(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (MerCountConfig) serializer.fromJsonFile(fs, file, MerCountConfig.class);
    }
    
    public MerCountConfig() {
        
    }
    
    public MerCountConfig(MerCountConfig config) {
        this.reportFilePath = config.reportFilePath;
        this.kmerSize = config.kmerSize;
        this.taskNum = config.taskNum;
        this.samplePaths.addAll(config.samplePaths);
        this.outputPath = config.outputPath;
    }

    @JsonProperty("kmer_size")
    public int getKmerSize() {
        return this.kmerSize;
    }
    
    @JsonProperty("kmer_size")
    public void setKmerSize(int kmerSize) {
        this.kmerSize = kmerSize;
    }
    
    @JsonProperty("task_num")
    public int getTaskNum() {
        return this.taskNum;
    }
    
    @JsonProperty("task_num")
    public void setTaskNum(int taskNum) {
        this.taskNum = taskNum;
    }
    
    @JsonProperty("sample_path")
    public Collection<String> getSamplePath() {
        return this.samplePaths;
    }
    
    @JsonProperty("sample_path")
    public void addSamplePaths(Collection<String> samplePaths) {
        this.samplePaths.addAll(samplePaths);
    }
    
    @JsonIgnore
    public void addSamplePath(String samplePath) {
        this.samplePaths.add(samplePath);
    }
    
    @JsonIgnore
    public void clearSamplePaths() {
        this.samplePaths.clear();
    }

    @JsonProperty("output_path")
    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }
    
    @JsonProperty("output_path")
    public String getOutputPath() {
        return this.outputPath;
    }
    
    @JsonProperty("report_path")
    public void setReportPath(String reportFilePath) {
        this.reportFilePath = reportFilePath;
    }
    
    @JsonProperty("report_path")
    public String getReportPath() {
        return this.reportFilePath;
    }
    
    @JsonIgnore
    public void saveTo(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        serializer.toJsonConfiguration(conf, HADOOP_CONFIG_KEY, this);
    }
    
    @JsonIgnore
    public void saveTo(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        serializer.toJsonFile(fs, file, this);
    }
}
