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
import java.nio.file.Paths;
import java.util.Iterator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Benchmark)
public class CursorIterationBlockFileBenchmark {

    private BlockFile blockFile;

    @Setup(Level.Iteration)
    public void setUp() throws IOException {
        blockFile = BlockFile.create(Paths.get("dupa"), 4096, 256);
    }

    @Benchmark
    public void cursorIterate(CursorState cursorState, Blackhole bh) {
        while (cursorState.cursor.hasNext()) {
            bh.consume(cursorState.cursor.next());
        }
    }

    @State(Scope.Thread)
    public static class CursorState {

        private Iterator<ByteBuffer> cursor;

        @Setup(Level.Iteration)
        public void setUp(CursorIterationBlockFileBenchmark b) {
            cursor = b.blockFile.iterator();
        }
    }
}
