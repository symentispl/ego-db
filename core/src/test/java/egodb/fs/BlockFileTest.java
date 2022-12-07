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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
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
        var readBuffer = ByteBuffer.allocate(blockSize);
        blockFile.read(readBuffer, 0);

        blockFile.read(readBuffer, 255);

        // than
        assertThat(readBuffer.array()).isEqualTo(writeBytes.array());
    }

    @Test
    void batchWriteSingleBlock(@TempDir Path tempDir) throws IOException {
        var blockFilePath = tempDir.resolve("block.dat");
        var blockSize = 4096;
        var numberOfBlocks = 256;
        var blockFile = BlockFile.create(blockFilePath, blockSize, numberOfBlocks);
        var randomGenerator = RandomGenerator.getDefault();
        var bytes = new byte[blockSize];
        randomGenerator.nextBytes(bytes);
        // when
        var writeBytes = ByteBuffer.wrap(bytes);
        blockFile.writeAll(new ByteBuffer[] {writeBytes}, new int[] {0});
        var readBuffer = ByteBuffer.allocate(blockSize);
        blockFile.read(readBuffer, 0);
        // than
        assertThat(readBuffer.array()).isEqualTo(writeBytes.array());
    }

    @Test
    void batchWriteMultipleBlocks(@TempDir Path tempDir) throws IOException {
        var blockFilePath = tempDir.resolve("block.dat");
        var blockSize = 4096;
        var numberOfBlocks = 256;
        var blockFile = BlockFile.create(blockFilePath, blockSize, numberOfBlocks);
        var randomGenerator = RandomGenerator.getDefault();
        var block0 = new byte[blockSize];
        randomGenerator.nextBytes(block0);
        var block1 = new byte[blockSize];
        randomGenerator.nextBytes(block1);
        // when
        blockFile.writeAll(new ByteBuffer[] {ByteBuffer.wrap(block0), ByteBuffer.wrap(block1)}, new int[] {0, 1});
        var readBuffer0 = ByteBuffer.allocate(blockSize);
        var readBuffer1 = ByteBuffer.allocate(blockSize);
        blockFile.read(readBuffer0, 0);
        blockFile.read(readBuffer1, 1);
        // than
        assertThat(readBuffer0.array()).isEqualTo(block0);
        assertThat(readBuffer1.array()).isEqualTo(block1);
    }

    @Test
    void batchWriteMultipleBlocksInterleavedWithSingle(@TempDir Path tempDir) throws IOException {
        var blockFilePath = tempDir.resolve("block.dat");
        var blockSize = 4096;
        var numberOfBlocks = 256;
        var blockFile = BlockFile.create(blockFilePath, blockSize, numberOfBlocks);
        var randomGenerator = RandomGenerator.getDefault();

        var block0 = new byte[blockSize];
        randomGenerator.nextBytes(block0);
        var block1 = new byte[blockSize];
        randomGenerator.nextBytes(block1);

        var block3 = new byte[blockSize];
        randomGenerator.nextBytes(block3);

        var block5 = new byte[blockSize];
        randomGenerator.nextBytes(block5);
        var block6 = new byte[blockSize];
        randomGenerator.nextBytes(block6);

        var block8 = new byte[blockSize];
        randomGenerator.nextBytes(block8);

        // when
        blockFile.writeAll(
                new ByteBuffer[] {
                    ByteBuffer.wrap(block0),
                    ByteBuffer.wrap(block1),
                    ByteBuffer.wrap(block3),
                    ByteBuffer.wrap(block5),
                    ByteBuffer.wrap(block6),
                    ByteBuffer.wrap(block8)
                },
                new int[] {0, 1, 3, 5, 6, 8});
        var readBuffer0 = ByteBuffer.allocate(blockSize);
        var readBuffer1 = ByteBuffer.allocate(blockSize);
        var readBuffer3 = ByteBuffer.allocate(blockSize);
        var readBuffer5 = ByteBuffer.allocate(blockSize);
        var readBuffer6 = ByteBuffer.allocate(blockSize);
        var readBuffer8 = ByteBuffer.allocate(blockSize);

        blockFile.read(readBuffer0, 0);
        blockFile.read(readBuffer1, 1);
        blockFile.read(readBuffer3, 3);
        blockFile.read(readBuffer5, 5);
        blockFile.read(readBuffer6, 6);
        blockFile.read(readBuffer8, 8);

        // than
        assertThat(readBuffer0.array()).isEqualTo(block0);
        assertThat(readBuffer1.array()).isEqualTo(block1);
        assertThat(readBuffer3.array()).isEqualTo(block3);
        assertThat(readBuffer5.array()).isEqualTo(block5);
        assertThat(readBuffer6.array()).isEqualTo(block6);
        assertThat(readBuffer8.array()).isEqualTo(block8);
    }
}
