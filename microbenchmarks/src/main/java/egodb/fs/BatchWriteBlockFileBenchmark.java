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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.stream.IntStream;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

@State(Scope.Benchmark)
@Fork(1)
public class BatchWriteBlockFileBenchmark {
    private static final int BLOCK_SIZE = 4096;

    @Param({"0,1,3,5,6,8", "0,1,2,3,4,5,6"})
    public String sequence;

    @Param({"NATIVE", "HEAP"})
    public String bufferFactoryType;

    private BlockFile blockFile;
    private int[] blocks;
    private ByteBuffer[] buffers;

    @Setup(Level.Iteration)
    public void setUp() throws IOException {

        var bufferFactory = BufferFactory.valueOf(bufferFactoryType);

        var file = Files.createTempFile("block", ".dat");

        blocks = Arrays.stream(this.sequence.split(","))
                .mapToInt(Integer::parseInt)
                .toArray();
        var bytes = new byte[BLOCK_SIZE];
        buffers = IntStream.range(0, blocks.length)
                .mapToObj(i -> bufferFactory.allocate(BLOCK_SIZE))
                .map(byteBuffer -> byteBuffer.put(bytes).flip())
                .toArray(ByteBuffer[]::new);

        long numberOfBlocks = 256;
        blockFile = BlockFile.create(file, BLOCK_SIZE, numberOfBlocks);
    }

    @Benchmark
    public void baseline() throws IOException {
        for (int i = 0; i < buffers.length; i++) {
            blockFile.write(buffers[i], blocks[i]);
            buffers[i].rewind();
        }
    }

    @Benchmark
    public void batch() throws IOException {
        blockFile.writeAll(buffers, blocks);
        for (int i = 0; i < buffers.length; i++) {
            buffers[i].rewind();
        }
    }
}
