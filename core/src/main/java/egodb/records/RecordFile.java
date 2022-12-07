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

import egodb.fs.BlockFile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.BitSet;

/**
 * A file which works as a heap of records.
 * <p>
 * This implementation has following properties:
 * <ul>
 *     <li>it supports variable size records, using block chaining</li>
 *     <li>updates are handled as deletes of previous versions and inserts of new record</li>
 *     <li>uses free-list to speed up inserts of new records</li>
 * </ul>
 * <p>
 * We have two types of blocks:
 * <ul>
 *     <li>card set, which holds bitset of free cards</li>
 *     <li>record set, which is set of records</li>
 * </ul>
 * <p>
 * How card set works?
 * <p>
 * Block is divided in n-byte cards, whenever we write we update bitset and mark occupied cards.
 * Whenever we delete record, we unmark freed cards.
 * <p>
 * Card sets are store every n record set, and hold free list for all subsequent record blocks.
 * <p>
 * For example for 4096 block size, card set block has following structure:
 * <ul>
 *     <li>byte header, which marks if this block is card set</li>
 *     <li>3-bytes of padding,</li>
 *     <li>4 bytes, to store bitset length</li>
 *     <li>remaining bytes are used as bit set which means we have available 4095*8 bits</li>
 * </ul>
 * (4096-8 (header size)) - gives us maximum size of bitset
 * (4088*8) gives us 32704 bits (cards)
 * if we assume bit card size of 64,
 * it gives us 511 blocks of records, so every 511 record blocks we will have single card set block
 * <p>
 * Records set blocks
 *
 * <ul>
 *     <li>4 bytes header, which marks if block is a card set</li>
 *     <li>set of records
 *          <ul>
 *              <li>first byte marks record type, it can be either RECORD, DELETED,OVERFLOW, NODE </li>
 *              <li>3-bytes padding</li>
 *              <li>if RECORD, next 4 bytes is length of record, and then we have record bytes</li>
 *              <li>if OVERFLOW, next 4 bytes are size of whole records, then 4 bytes which is number of nodes</li>
 *              <li>if NODE, next 4 bytes is size of node in bytes then number of bytes, then 4 bytes address of overflow record which is root of this chain</li>
 *          </ul>
 *     </li>
 * </ul>
 */
public class RecordFile implements AutoCloseable
{

    private final BlockFile blockFile;

    private RecordFile( BlockFile blockFile )
    {
        this.blockFile = blockFile;
    }

    public static RecordFile create( Path path, int blockSize, long numberOfBlocks ) throws IOException
    {
        var blockFile = BlockFile.create( path, blockSize, numberOfBlocks );
        // create first card set block
        var buffer = ByteBuffer.allocate( blockSize );
        CardSet.create( buffer );
        blockFile.write( buffer, 0 );
        return new RecordFile( blockFile );
    }

    public static RecordFile open( Path path ) throws IOException
    {
        var blockFile = BlockFile.open( path );
        // in a normal situation we would check if file was closed properly,
        // and try to recover if needed
        return new RecordFile( blockFile );
    }

    public void insert( ByteBuffer buffer )
    {

        // prepare entries for writing
        var remaining = buffer.remaining();
//        if(remaining>blockFile.header().blockSize()-RecordEntry.Header.size()){
//            // split and prepare chain of entries
//        } else{
//            // prepare single record
//        }

        // find next free entry in record set
        var cursor = blockFile.iterator();

        do
        {
            if ( !cursor.hasNext() )
            {
                // we don't have enough blocks, append new one
                // check if it should be card set block
                // or record set block
                // then write new record
            }
            else
            {
                // var remaining = buffer.remaining();
                // if bigger then block size-RecordEntry.header(), this will be overflow
                // otherwise search for first block that fits
            }
        }
        while ( cursor.hasNext() );
    }

    public BitSet cardSet( int block ) throws IOException
    {
        var buffer = ByteBuffer.allocate( blockFile.header().blockSize() );
        blockFile.read( buffer, block );

        var mark = buffer.get( 0 );
        if ( mark != BlockType.CardSet.mark() )
        {
            throw new IllegalStateException();
        }

        return BitSet.valueOf( buffer.slice( 1, blockFile.header().blockSize()-1 ) );
    }

    @Override
    public void close() throws Exception
    {
        blockFile.close();
    }

    enum BlockType
    {
        RecordSet( (byte) 0x01 ),
        CardSet( (byte) 0x03 );

        private final byte mark;

        BlockType( byte mark )
        {
            this.mark = mark;
        }

        byte mark()
        {
            return mark;
        }
    }

    class Header
    {
        private final static int MARK_OFFSET = 0;
        private final static int MARK_SIZE = Byte.BYTES;
    }
}
