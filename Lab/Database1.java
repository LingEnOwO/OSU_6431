import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Database1 {

    private Row rows[] = new Row[100];
    private List<Operation> operationLog = new ArrayList<>();

    public Database1() {
        for (int i = 0; i < 100; i++) {
            rows[i] = new Row(i);
        }
    }

    public void executeTransactions(List<Transaction> transactions) {
    List<Thread> threads = new ArrayList<>();

    for (Transaction t : transactions) {
        Thread transactionThread = new Thread(() -> {
            List<ReentrantReadWriteLock> acquiredLocks = new ArrayList<>();
            boolean allLocksAcquired = false;

            try {
                // Step 1: Acquire all locks for the transaction upfront (Strict 2PL)
                for (Operation o : t.getOperations()) {
                    ReentrantReadWriteLock lock = rows[o.getRowNumber()].getLock();
                    if (o.getType() == 0) { // READ operation
                        lock.readLock().lock();
                    } else { // WRITE operation
                        lock.writeLock().lock();
                    }
                    acquiredLocks.add(lock); // Track acquired locks
                }
                allLocksAcquired = true;

                // Step 2: Execute all operations after acquiring locks
                for (Operation o : t.getOperations()) {
                    synchronized (operationLog) {
                        operationLog.add(o); // Log each operation for serializability testing
                    }
                    if (o.getType() == 0) { // READ operation
                        o.setValue(rows[o.getRowNumber()].getValue());
                    } else { // WRITE operation
                        rows[o.getRowNumber()].setValue(o.getValue());
                    }
                    System.out.println("T" + t.getId() + " is executing " + o);
                }

            } finally {
                // Step 3: Release all locks at the end of the transaction (Strict 2PL)
                if (allLocksAcquired) {
                    for (ReentrantReadWriteLock lock : acquiredLocks) {
                        if (lock.isWriteLockedByCurrentThread()) {
                            lock.writeLock().unlock();
                        } else {
                            lock.readLock().unlock();
                        }
                    }
                }
            }
        });

        // Start the transaction thread and add it to the list of threads
        threads.add(transactionThread);
        transactionThread.start();
    }

    // Wait for all threads to complete
    for (Thread thread : threads) {
        try {
            thread.join(); // Ensure each thread finishes before moving on
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

    public List<Operation> getOperationLog() {
        return operationLog;
    }

    public Row[] getRowsCopy() {
        Row[] copy = new Row[rows.length];
        for (int i = 0; i < rows.length; i++) {
            copy[i] = new Row(rows[i].getValue());
        }
        return copy;
    }

    public void serialExecution(List<Transaction> transactions) {
        //System.out.println("Serial Execution:");
        for (Transaction t : transactions) {
            for (Operation o : t.getOperations()) {
                //System.out.println("T" + t.getId() + " is executing " + o);
                if (o.getType() == 0) { // READ operation
                    o.setValue(rows[o.getRowNumber()].getValue());
                } else { // WRITE operation
                    rows[o.getRowNumber()].setValue(o.getValue());
                }
            }
        }
    }

    public static void main(String[] args) {
        Database1 db = new Database1();

        // Define transactions
        Transaction t1 = new Transaction(1);
        //t1.addOperation(0, 3, 0); // READ row 3
        //t1.addOperation(1, 4, 50); // WRITE row 4 value 50
        //t1.addOperation(0, 5, 0); // READ row 5
        t1.addOperation(1, 1, 50);
        t1.addOperation(0, 2, 0);

        Transaction t2 = new Transaction(2);
        //t2.addOperation(1, 3, 75); // WRITE row 3 value 75
        //t2.addOperation(0, 4, 0); // READ row 4
        //t2.addOperation(1, 5, 30); // WRITE row 5 value 30
        t2.addOperation(1, 2, 7);
        t2.addOperation(0, 3, 0);

        Transaction t3 = new Transaction(3);
        //t3.addOperation(0, 2, 0); // READ row 2
        //t3.addOperation(1, 1, 20); // WRITE row 1 value 20
        //t3.addOperation(0, 4, 0); // READ row 4
        t3.addOperation(1, 3, 10);
        t3.addOperation(0, 1, 0);
        // Add transactions to list
        LinkedList<Transaction> batch = new LinkedList<>();
        batch.add(t1);
        batch.add(t2);
        batch.add(t3);
        // Execute transactions concurrently using threads
        db.executeTransactions(batch);
        Row[] concurrentState = db.getRowsCopy();
        // Verify serializability
        Test test = new Test();
        boolean res = test.verifySerializability(batch, concurrentState);
        
        if (res){
            System.out.println("The concurrent execution is equivalent to a serial log.");
        }
        else{
            System.out.println("The concurrent execution is not equivalent to any serial log.");
        }
        SerializationGraph graph = new SerializationGraph();
        boolean isSerializable = graph.verifySerializability(db.getOperationLog());
        if (isSerializable) {
            System.out.println("The concurrent execution is serializable.");
        } else {
            System.out.println("The concurrent execution is NOT serializable.");
        }
    }
}
