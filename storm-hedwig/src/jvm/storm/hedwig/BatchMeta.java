package storm.hedwig;

public class BatchMeta {
    long offset;
    long nextOffset;
    public BatchMeta(long offset, long nextOffset) {
        this.offset = offset;
        this.nextOffset = nextOffset;
    }
}
