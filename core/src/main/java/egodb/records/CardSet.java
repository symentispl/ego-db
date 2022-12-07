package egodb.records;

import java.nio.ByteBuffer;

/**
 * Card set has following structure:
 * <ul>
 *  <li>mark - 1 byte</li>
 * <li>bitset size - 4 bytes</li>
 * <li>padding (to size of single card) - card size-(mark+bitset size)</li>
 * <li>bitset</li>
 * </ul>
 */
public class CardSet
{
    public static void create( ByteBuffer buffer )
    {
        buffer.put( 0, RecordFile.BlockType.CardSet.mark() );
        // rest of card set are unset bits
    }

    static class Header
    {
        private final byte mark =
    }
}
