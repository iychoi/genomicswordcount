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

/**
 *
 * @author iychoi
 */
public class MerCountConfigException extends Exception {
    static final long serialVersionUID = 7818375828146090155L;

    public MerCountConfigException() {
        super();
    }

    public MerCountConfigException(String string) {
        super(string);
    }

    public MerCountConfigException(String string, Throwable thrwbl) {
        super(string, thrwbl);
    }

    public MerCountConfigException(Throwable thrwbl) {
        super(thrwbl);
    }
}
