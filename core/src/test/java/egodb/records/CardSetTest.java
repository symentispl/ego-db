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

import static egodb.HexDumpSupport.toHexDump;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class CardSetTest {

    @Test
    void createCardSet() {
        // given
        var blockSize = 4096;
        short cardSize = 64;
        var buffer = ByteBuffer.allocate(blockSize);
        // when
        var cardSet = CardSet.create(buffer, cardSize);
        // then
        assertThat(cardSet.bitSetSize()).isEqualTo((buffer.capacity() - cardSize) * 8);
        assertThat(buffer.get(RecordFile.Header.Fields.Mark.position()))
                .as("buffer content is %s", toHexDump(buffer, cardSize))
                .isEqualTo(RecordFile.BlockType.CardSet.mark());
        assertThat(buffer.getInt(CardSet.Header.Fields.BitSetSize.position()))
                .as("buffer content is %s", toHexDump(buffer, cardSize))
                .isEqualTo((buffer.capacity() - cardSize) * 8);
    }

    @Test
    void flushBitSet() {
        var blockSize = 4096;
        short cardSize = 64;
        var buffer = ByteBuffer.allocate(blockSize);
        var cardSet = CardSet.create(buffer, cardSize);
        // when
        // set first byte
        cardSet.set(0, 8);
        cardSet.flush();
        // then
        assertThat(buffer.get(RecordFile.Header.Fields.Mark.position()))
                .as("buffer content is %s", toHexDump(buffer, cardSize))
                .isEqualTo(RecordFile.BlockType.CardSet.mark());
        assertThat(buffer.getInt(CardSet.Header.Fields.BitSetSize.position()))
                .as("buffer content is %s", toHexDump(buffer, cardSize))
                .isEqualTo((buffer.capacity() - cardSize) * 8);
        assertThat(buffer.get(cardSize))
                .as("buffer content is %s", toHexDump(buffer, cardSize))
                .isEqualTo((byte) -1);
        // when
        // set first byte
        cardSet.clear(0, 8);
        cardSet.flush();
        // then
        assertThat(buffer.get(RecordFile.Header.Fields.Mark.position()))
                .as("buffer content is %s", toHexDump(buffer, cardSize))
                .isEqualTo(RecordFile.BlockType.CardSet.mark());
        assertThat(buffer.getInt(CardSet.Header.Fields.BitSetSize.position()))
                .as("buffer content is %s", toHexDump(buffer, cardSize))
                .isEqualTo((buffer.capacity() - cardSize) * 8);
        assertThat(buffer.get(cardSize)).isEqualTo((byte) 0);
    }
}
