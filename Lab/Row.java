import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Row{
    int value;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();


    public Row(int value){
        this.value = value;
    }

    public int getValue(){
        return this.value;
    }

    public void setValue(int value){
        this.value = value;
    }

    public ReentrantReadWriteLock getLock(){
        return lock;
    }
}
