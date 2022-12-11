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

import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * Card set has following structure:
 * <ul>
 *  <li>mark - 1 byte</li>
 * <li>bitset size - 4 bytes</li>
 * <li>padding (to size of single card) - card size-(mark+bitset size)</li>
 * <li>bitset</li>
 * </ul>
 */
public class CardSet {

    /* Used to shift left or right for a partial word mask */
    private static final long WORD_MASK = 0xffffffffffffffffL;
    private static final int ADDRESS_BITS_PER_WORD = 6;

    private final Header header;
    private final ByteBuffer buffer;
    private final BitSet bitSet;

    public CardSet(Header header, ByteBuffer buffer, BitSet bitSet) {

        this.header = header;
        this.buffer = buffer;
        this.bitSet = bitSet;
    }

    public static CardSet create(ByteBuffer buffer, short cardSize) {
        buffer.put(RecordFile.Header.Fields.Mark.position(), RecordFile.BlockType.CardSet.mark());
        var header = new Header(buffer.capacity(), cardSize);
        // bit set size should be a part of block file header, now card set block as it is constant for whole file
        buffer.putInt(Header.Fields.BitSetSize.position(), header.bitSetSize());
        // rest of card set are unset bits
        var bitSet = new BitSet(header.bitSetSize());
        // BitSet.valueOf(

        return new CardSet(header, buffer, bitSet);
    }

    private static int wordIndex(int bitIndex) {
        return bitIndex >> ADDRESS_BITS_PER_WORD;
    }

    public int bitSetSize() {
        return header.bitSetSize();
    }

    public void set(int fromIndex, int toIndex) {
        //        int startWordIndex = wordIndex(fromIndex);
        //        int endWordIndex = wordIndex(toIndex - 1);
        //        long firstWordMask = WORD_MASK << fromIndex;
        //        long lastWordMask = WORD_MASK >>> -toIndex;
        //        var longBuffer = buffer.asLongBuffer();
        //        if (startWordIndex == endWordIndex) {
        //            // Case 1: One word
        //            var currentWord = longBuffer.get(startWordIndex);
        //            longBuffer.put(startWordIndex, currentWord | (firstWordMask & lastWordMask));
        //        } else {
        //            // Case 2: Multiple words
        //            // Handle first word
        //
        //            words[startWordIndex] |= firstWordMask;
        //
        //            // Handle intermediate words, if any
        //            for (int i = startWordIndex + 1; i < endWordIndex; i++) words[i] = WORD_MASK;
        //
        //            // Handle last word (restores invariants)
        //            words[endWordIndex] |= lastWordMask;
        //        }

        bitSet.set(fromIndex, toIndex);
    }

    public void clear(int fromIndex, int toIndex) {
        bitSet.clear(fromIndex, toIndex);
    }

    public void flush() {
        // PERFORMANCE: extra copying of things, we couldn't find bitset which works on bytebuffer
        var src = bitSet.toByteArray();
        var copyArray = new byte[header.bitSetSize() / Byte.SIZE];
        System.arraycopy(src, 0, copyArray, 0, src.length);
        buffer.put(header.cardSize, copyArray);
    }

    static class Header {
        private final int blockSize;
        private final short cardSize;

        public Header(int blockSize, short cardSize) {
            // enforce power of two for both fields
            this.blockSize = blockSize;
            this.cardSize = cardSize;
        }

        public int bitSetSize() {
            return (blockSize - cardSize) * Byte.SIZE;
        }

        enum Fields implements HeaderField {
            BitSetSize(RecordFile.Header.Fields.Mark, Integer.BYTES),
            Padding(BitSetSize, Integer.BYTES);

            private final int position;
            private final int size;

            Fields(int position, int size) {

                this.position = position;
                this.size = size;
            }

            Fields(HeaderField prevField, int size) {
                this(prevField.position() + prevField.size(), size);
            }

            @Override
            public int position() {
                return position;
            }

            @Override
            public int size() {
                return size;
            }
        }
    }
}
