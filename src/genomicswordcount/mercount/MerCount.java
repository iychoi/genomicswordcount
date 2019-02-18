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

import genomicswordcount.common.cmdargs.CommandArgumentsParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author iychoi
 */
public class MerCount extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(MerCount.class);
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MerCount(), args);
        System.exit(res);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration common_conf = this.getConf();
        CommandArgumentsParser<MerCountCmdArgs> parser = new CommandArgumentsParser<MerCountCmdArgs>();
        MerCountCmdArgs cmdParams = new MerCountCmdArgs();
        if(!parser.parse(args, cmdParams)) {
            LOG.error("Failed to parse command line arguments!");
            return 1;
        }
        
        if(cmdParams.isHelp()) {
            printHelp();
            return 1;
        }
        
        MerCountConfig mcConfig = cmdParams.getMerCountConfig();
        
        int res = 0;
        try {
            MerCountMR merCountMR = new MerCountMR();
            res = merCountMR.runJob(new Configuration(common_conf), mcConfig);
            if(res != 0) {
                throw new Exception("MerCountMR Failed : " + res);
            }
        } catch (Exception e) {
            LOG.error(e);
            e.printStackTrace();
            res = 1;
        }
        
        return res;
    }

    private static void printHelp() {
        System.out.println("============================================================");
        System.out.println("MerCount");
        System.out.println("============================================================");
        System.out.println("Usage :");
        System.out.println("> mercount <arguments ...>");
    }
}
