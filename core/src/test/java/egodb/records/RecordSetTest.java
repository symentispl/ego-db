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
package egodb.records;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
class RecordSetTest {

    @Test
    void invalidRecordSetBlock() {
        var buffer = ByteBuffer.allocate(4096);
        assertThatThrownBy(() -> RecordSet.cursor(buffer)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void appendRecordToSet() {
        var buffer = ByteBuffer.allocate(4096);
        // buffer.put( 0, RecordSet.RECORD_SET_MARK );
        buffer.putShort(1, (short) 3);

        var cursor = RecordSet.cursor(buffer);
        var record = "FIRST".getBytes(Charset.defaultCharset());
        cursor.append(record);
        System.out.println(RecordSet.prettyPrint(buffer));
        assertThat(cursor).hasNext();
        assertThat(cursor.next()).isEqualTo(record);
    }
}
