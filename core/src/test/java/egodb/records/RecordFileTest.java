package egodb.records;

import egodb.fs.BlockFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;

import static org.assertj.core.api.Assertions.assertThat;

class RecordFileTest
{

    @Test
    void createAndOpen( @TempDir Path tempDir ) throws Exception
    {
        // given
        var path = Files.createTempFile( tempDir, "data", ".dat" );
        // when
        var blockSize = 4096;
        var numberOfBlocks = 256;
        try ( var ignored = RecordFile.create( path, blockSize, numberOfBlocks ) )
        {

        }
        // then
        var buffer = ByteBuffer.allocate( blockSize );
        try ( var blockFile = BlockFile.open( path ) )
        {
            assertThat( blockFile.read( buffer, 0 ) ).isTrue();
            assertThat( buffer.array() ).startsWith( RecordFile.BlockType.CardSet.mark() );
        }
        // when
        try ( var recordFile = RecordFile.open( path ) )
        {
            BitSet cardSet = recordFile.cardSet( 0 );
            assertThat( cardSet.isEmpty() ).isTrue();
        }
    }
}