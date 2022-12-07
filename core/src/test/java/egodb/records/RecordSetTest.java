package egodb.records;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecordSetTest
{

    @Test
    void invalidRecordSetBlock()
    {
        var buffer = ByteBuffer.allocate( 4096 );
        assertThatThrownBy( () -> RecordSet.cursor( buffer ) )
                .isInstanceOf( IllegalArgumentException.class );
    }

    @Test
    void appendRecordToSet()
    {
        var buffer = ByteBuffer.allocate( 4096 );
        //buffer.put( 0, RecordSet.RECORD_SET_MARK );
        buffer.putShort( 1, (short)3 );



        var cursor = RecordSet.cursor( buffer );
        var record = "FIRST".getBytes( Charset.defaultCharset() );
        cursor.append( record );
        System.out.println(RecordSet.prettyPrint(buffer));
        assertThat( cursor ).hasNext();
        assertThat( cursor.next() ).isEqualTo( record );
    }
}