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

import java.util.ArrayList;
import java.util.List;
import genomicswordcount.common.cmdargs.CommandArgumentsBase;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

/**
 *
 * @author iychoi
 */
public class MerCountCmdArgs extends CommandArgumentsBase {
    
    public MerCountCmdArgs() {
        
    }
    
    @Option(name = "-k", aliases = "--kmersize", usage = "specify kmer size")
    protected int kmerSize = MerCountConfig.DEFAULT_KMERSIZE;

    public int getKmerSize() {
        return this.kmerSize;
    }
    
    @Option(name = "-t", aliases = "--tasks", usage = "specify number of tasks/reducers")
    protected int taskNum = MerCountConfig.DEFAULT_TASKNUM;
    
    public int getTaskNum() {
        return this.taskNum;
    }
    
    @Option(name = "-o", usage = "specify preprocess output path")
    private String outputPath = MerCountConfig.DEFAULT_OUTPUT_PATH;
        
    public String getOutputPath() {
        return this.outputPath;
    }
    
    @Argument(metaVar = "input-path [input-path ...]", usage = "input-paths")
    private List<String> inputPaths = new ArrayList<String>();

    public String[] getInputPaths() {
        if(this.inputPaths.isEmpty()) {
            return new String[0];
        }

        return this.inputPaths.toArray(new String[0]);
    }

    public String getCommaSeparatedInputPath() {
        String[] inputPaths = getInputPaths();
        StringBuilder CSInputPath = new StringBuilder();
        for(String inputpath : inputPaths) {
            if(CSInputPath.length() != 0) {
                CSInputPath.append(",");
            }
            CSInputPath.append(inputpath);
        }
        return CSInputPath.toString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(String arg : this.inputPaths) {
            if(sb.length() != 0) {
                sb.append(", ");
            }

            sb.append(arg);
        }

        return "paths = " + sb.toString();
    }

    @Override
    public boolean checkValidity() {
        if(!super.checkValidity()) {
           return false;
        }
        
        if(this.kmerSize <= 0 ||
                this.taskNum < 0 ||
                this.outputPath == null ||
                this.inputPaths == null || 
                this.inputPaths.isEmpty() || 
                this.inputPaths.size() < 1) {
            return false;
        }
        
        return true;
    }
    
    public MerCountConfig getMerCountConfig() {
        MerCountConfig config = new MerCountConfig();
        
        config.setReportPath(this.reportfile);
        config.setKmerSize(this.kmerSize);
        config.setTaskNum(this.taskNum);
        config.addSamplePaths(this.inputPaths);
        config.setOutputPath(this.outputPath);
        return config;
    }
}
