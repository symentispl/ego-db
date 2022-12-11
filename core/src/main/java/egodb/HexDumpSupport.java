/*
 * Copyright © 2022 Symentis.pl (jaroslaw.palka@symentis.pl)
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
package egodb;

import java.nio.ByteBuffer;
import java.util.HexFormat;
import java.util.StringJoiner;

public class HexDumpSupport {
    public static String toHexDump(ByteBuffer buffer, int cardSize) {
        var hexDump = new StringJoiner("\n");
        var hexFormat = HexFormat.ofDelimiter(" ");
        var array = buffer.array();
        for (int i = 0; i < buffer.capacity(); i = i + cardSize) {
            hexDump = hexDump.add(hexFormat.formatHex(array, i, i + cardSize - 1));
        }
        return hexDump.toString();
    }
}
