/*
 * Copyright 2016 iychoi.
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

import java.io.IOException;
import genomicswordcount.common.helpers.SequenceHelper;
import genomicswordcount.common.sequence.ReadInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author iychoi
 */
public class MerCountMRMapper extends Mapper<LongWritable, ReadInfo, Text, LongWritable> {
    
    private static final Log LOG = LogFactory.getLog(MerCountMRMapper.class);
    
    private MerCountConfig mcConfig;
    private int kmerSize = 0;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.mcConfig = MerCountConfig.createInstance(conf);
        this.kmerSize = this.mcConfig.getKmerSize();
    }
    
    @Override
    protected void map(LongWritable key, ReadInfo value, Context context) throws IOException, InterruptedException {
        //LOG.info("Mapper : " + value.getDescription());
        Text outKey = new Text();
        LongWritable outVal = new LongWritable();
                
        String sequence = value.getSequence();
        if(sequence.length() >= this.kmerSize) {
            sequence = sequence.toUpperCase();
            
            boolean pvalid = false;
            for (int i = 0; i < (sequence.length() - this.kmerSize + 1); i++) {
                String kmer = sequence.substring(i, i + this.kmerSize);
                if (pvalid) {
                    if (!SequenceHelper.isValidSequence(kmer.charAt(this.kmerSize - 1))) {
                        //LOG.info("discard invalid kmer sequence : " + kmer);
                        pvalid = false;
                        continue;
                    } else {
                        pvalid = true;
                    }
                } else {
                    if (!SequenceHelper.isValidSequence(kmer)) {
                        //LOG.info("discard invalid kmer sequence : " + kmer);
                        pvalid = false;
                        continue;
                    } else {
                        pvalid = true;
                    }
                }

                //String canonicalKmer = SequenceHelper.canonicalize(kmer);
                outKey.clear();
                outKey.set(kmer);
                
                outVal.set(1);
                
                context.write(outKey, outVal);
            }
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
