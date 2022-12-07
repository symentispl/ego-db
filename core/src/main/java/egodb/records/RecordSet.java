package egodb.records;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static java.lang.String.format;

/**
 * A record set has following structure:
 * <ul>
 *     <li>block type mark - 1 byte</li>
 *     <li>next record position - 2 byte</li>
 *     <li>set of records, where every record will have
 *          <ul>record type - 1 byte (RECORD,DELETED,OVERFLOW)</ul>
 *          <ul>record length - 2 bytes</ul>
 *          <ul>record bytes - size of record</ul>
 *     </li>
 * </ul>
 */
public class RecordSet
{

    private final byte type = RecordFile.BlockType.RecordSet.mark();
    private Entry[] entries;

    public static Cursor cursor( ByteBuffer buffer )
    {
        return new Cursor( buffer );
    }

    static String prettyPrint( ByteBuffer buffer )
    {
        StringBuilder s = new StringBuilder();
        s.append( format( "mark: %#04x\n", buffer.get( 0 ) ) );
        s.append( format( "nextRecordPosition: %d\n" , Short.toUnsignedInt( buffer.getShort(1) )));



//        while(){
//
//        }
        return s.toString();
    }

    private sealed interface Entry permits Record
    {
        enum Type
        {
            Record( (byte) 0x01 );

            private final byte mark;

            Type( byte mark )
            {
                this.mark = mark;
            }

            byte mark()
            {
                return mark;
            }
        }
    }

    public static class Cursor implements Iterator<byte[]>
    {
        private ByteBuffer buffer;
        private int nextRecordPosition;
        private int cursorPosition;

        private Cursor( ByteBuffer buffer )
        {
            reset( buffer );
        }

        private void reset( ByteBuffer buffer )
        {
            // begining of header
            // read first byte and check block type
            nextRecordPosition = 0;
            cursorPosition = 0;
            var type = buffer.get( nextRecordPosition );
//            if ( type != RECORD_SET_MARK )
//            {
//                throw new IllegalArgumentException( "not a record set" );
//            }
//            nextRecordPosition += Byte.BYTES;

            // read next free position
            nextRecordPosition = Short.toUnsignedInt( buffer.getShort( nextRecordPosition ) );
            if ( nextRecordPosition >= buffer.capacity() )
            {
                throw new IllegalArgumentException( "next record position outside of buffer boundaries" );
            }

            // move cursor position to end of header
            cursorPosition += Byte.BYTES;
            cursorPosition += Short.BYTES;
            // end of header

            this.buffer = buffer;
        }

        public void append( byte[] record )
        {
            // TODO bounds checks
            buffer.put( nextRecordPosition, Entry.Type.Record.mark() );
            nextRecordPosition += Byte.BYTES;
            buffer.putShort( nextRecordPosition, (short) record.length );
            nextRecordPosition += Short.BYTES;
            buffer.put( nextRecordPosition, record );
            nextRecordPosition += record.length;
        }

        @Override
        public boolean hasNext()
        {
            var hasNext = buffer.get( cursorPosition ) == Entry.Type.Record.mark();
            if ( hasNext )
            {
                return true;
            }
            else
            {
                // skip DELETED until RECORD found or end of buffer reached
            }
            return false;
        }

        @Override
        public byte[] next()
        {
            if ( hasNext() )
            {
                // read next record
            }
            throw new NoSuchElementException();
        }
    }

    private final class Record implements Entry
    {

    }
}
