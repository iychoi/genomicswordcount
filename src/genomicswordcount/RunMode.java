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
package genomicswordcount;

/**
 *
 * @author iychoi
 */
public enum RunMode {
    MERCOUNT;
    
    private final static String MERCOUNT_MATCH_STRINGS[] = {"mer", "mercount"};
    
    public static RunMode fromString(String alg) {
        try {
            RunMode wa = RunMode.valueOf(alg.trim().toUpperCase());
            return wa;
        } catch (Exception ex) {
            // fall
        }
        
        // compare to a list of match strings and return a run mode
        
        // preprocess match strings
        for(String match : MERCOUNT_MATCH_STRINGS) {
            if(match.equalsIgnoreCase(alg.trim())) {
                return MERCOUNT;
            }
        }
        
        return null;
    }
}
