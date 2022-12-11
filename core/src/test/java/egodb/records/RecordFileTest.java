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

import egodb.fs.BlockFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RecordFileTest {

    @Test
    @Disabled
    void createAndOpen(@TempDir Path tempDir) throws Exception {
        // given
        var path = Files.createTempFile(tempDir, "data", ".dat");
        // when
        var blockSize = 4096;
        var numberOfBlocks = 256;
        try (var ignored = RecordFile.create(path, blockSize, numberOfBlocks)) {}

        // then
        var buffer = ByteBuffer.allocate(blockSize);
        try (var blockFile = BlockFile.open(path)) {
            assertThat(blockFile.read(buffer, 0)).isTrue();
            assertThat(buffer.array()).startsWith(RecordFile.BlockType.CardSet.mark());
        }
        // when
        try (var recordFile = RecordFile.open(path)) {
            BitSet cardSet = recordFile.cardSet(0);
            assertThat(cardSet.isEmpty()).isTrue();
        }
    }
}
