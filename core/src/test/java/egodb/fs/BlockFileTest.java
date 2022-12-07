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
package egodb.fs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.random.RandomGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BlockFileTest {
    private static void assertBlockFileHeader(Path path, int blockSize, int numberOfBlocks, BlockFile blockFile) {
        assertThat(path).isRegularFile().hasSize((numberOfBlocks + 1) * 4096);
        var header = blockFile.header();
        assertThat(header.version()).isEqualTo(1);
        assertThat(header.blockSize()).isEqualTo(blockSize);
        assertThat(header.numberOfBlocks()).isEqualTo(numberOfBlocks);
    }

    @Test
    void createAndOpenBlockFile(@TempDir Path tempDir) throws IOException {
        // given
        var blockFilePath = tempDir.resolve("block.dat");
        var blockSize = 4096;
        var numberOfBlocks = 256;
        // when
        var newBlockFile = BlockFile.create(blockFilePath, blockSize, numberOfBlocks);
        // than
        assertBlockFileHeader(blockFilePath, blockSize, numberOfBlocks, newBlockFile);
        // when
        var blockFile = BlockFile.open(blockFilePath);
        assertBlockFileHeader(blockFilePath, blockSize, numberOfBlocks, blockFile);
    }

    @Test
    void readAndWriteBlockFile(@TempDir Path tempDir) throws IOException {
        // given
        var blockFilePath = tempDir.resolve("block.dat");
        var blockSize = 4096;
        var numberOfBlocks = 256;
        var blockFile = BlockFile.create(blockFilePath, blockSize, numberOfBlocks);
        var randomGenerator = RandomGenerator.getDefault();
        var bytes = new byte[blockSize];
        randomGenerator.nextBytes(bytes);
        // when
        var writeBytes = ByteBuffer.wrap(bytes);
        blockFile.write(writeBytes, 0);
        var readBuffer0 = ByteBuffer.allocate(blockSize);
        blockFile.read(readBuffer0, 0);
        // than
        assertThat(readBuffer0.array()).isEqualTo(writeBytes.array());
        // when
        var readBuffer255 = ByteBuffer.allocate(blockSize);
        blockFile.read(readBuffer255, 255);
        // than
        assertThat(readBuffer255.array()).containsOnly(0);
    }

    @Test
    void iteratingOverCursorWithHasNext(@TempDir Path tempDir) throws IOException {
        // given
        var blockFilePath = tempDir.resolve("block.dat");
        var blockSize = 4096;
        var numberOfBlocks = 8;
        var blockFile = BlockFile.create(blockFilePath, blockSize, numberOfBlocks);
        var randomGenerator = RandomGenerator.getDefault();

        var buffers = new ByteBuffer[numberOfBlocks];
        for (int i = 0; i < numberOfBlocks; i++) {
            var bytes = new byte[blockSize];
            randomGenerator.nextBytes(bytes);
            // when
            var writeBytes = ByteBuffer.wrap(bytes);
            buffers[i] = writeBytes;
            blockFile.write(writeBytes, i);
        }

        var cursor = blockFile.iterator();
        for (int i = 0; i < numberOfBlocks; i++) {
            assertThat(cursor).hasNext();
            assertThat(cursor.next()).isEqualTo(buffers[i]);
        }
        assertThat(cursor).isExhausted();
    }

    @Test
    void iteratingOverCursorWithNext(@TempDir Path tempDir) throws IOException {
        // given
        var blockFilePath = tempDir.resolve("block.dat");
        var blockSize = 4096;
        var numberOfBlocks = 8;
        var blockFile = BlockFile.create(blockFilePath, blockSize, numberOfBlocks);
        var randomGenerator = RandomGenerator.getDefault();

        var buffers = new ByteBuffer[numberOfBlocks];
        for (int i = 0; i < numberOfBlocks; i++) {
            var bytes = new byte[blockSize];
            randomGenerator.nextBytes(bytes);
            // when
            var writeBytes = ByteBuffer.wrap(bytes);
            buffers[i] = writeBytes;
            blockFile.write(writeBytes, i);
        }

        var cursor = blockFile.iterator();
        for (int i = 0; i < numberOfBlocks; i++) {
            assertThat(cursor.next()).isEqualTo(buffers[i]);
        }
        assertThatThrownBy(() -> cursor.next()).isInstanceOf(NoSuchElementException.class);
    }
}
