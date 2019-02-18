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
package genomicswordcount.common.sequence;

/**
 *
 * @author iychoi
 */
public class ReadInfo {
    private String filename;
    private String description;
    private String sequence;
    
    public ReadInfo(String filename) {
        this.filename = filename;
    }
    
    public ReadInfo(String filename, Read read) {
        this.filename = filename;
        this.description = read.getDescription();
        this.sequence = read.getFullSequence();
    }

    public String getFilename() {
        return this.filename;
    }
    
    public void setDescription(String description) {
        this.description = description;
    }
    
    public String getDescription() {
        return this.description;
    }
    
    public void setSequence(String sequence) {
        this.sequence = sequence;
    }
    
    public String getSequence() {
        return this.sequence;
    }
}
