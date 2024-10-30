import java.util.List;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Database2 {

    private Row rows[] = new Row[100];
    private final int NUM_THREADS = 10;
    private final int PARTITION_SIZE = 10;
    private List<BlockingQueue<Transaction>> queues = new LinkedList<>();;

    public Database2() {
        for (int i = 0; i < 100; i++) {
            rows[i] = new Row(i);
        }

        //queues = new LinkedList<>();
        for (int i = 0; i < NUM_THREADS; i++) {
            queues.add(new LinkedBlockingQueue<>()); // Dynamic capacity
        }
    }

    public void executeTransactions(List<Transaction> transactions) {
        // Create and start threads
        System.out.println("Pipeline Execution:");
        for (int i = 0; i < NUM_THREADS; i++) {
            final int threadId = i;
            new Thread(() -> processTransactions(threadId)).start();
        }

        // Enqueue all transactions into the first queue to begin processing
        try {
            for (Transaction t : transactions) {
                queues.get(0).put(t);
            }

            // Add a termination signal (poison pill) with ID -1
            queues.get(0).put(new Transaction(-1));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void processTransactions(int threadId) {
        BlockingQueue<Transaction> inputQueue = queues.get(threadId);
        BlockingQueue<Transaction> outputQueue = threadId < NUM_THREADS - 1 ? queues.get(threadId + 1) : null;

        try {
            while (true) {
                Transaction transaction = inputQueue.take(); // Take transaction from queue

                // Check for the termination signal (poison pill)
                if (transaction.getId() == -1) { // If ID is -1, it's the poison pill
                    if (outputQueue != null) {
                        outputQueue.put(transaction); // Pass poison pill to the next queue
                    }
                    break; // Exit the loop and terminate the thread
                }

                // Process operations for this thread's partition of rows
                for (Operation o : transaction.getOperations()) {
                    int rowNumber = o.getRowNumber();
                    if (rowNumber >= threadId * PARTITION_SIZE && rowNumber < (threadId + 1) * PARTITION_SIZE) {
                        System.out.println("T" + transaction.getId() + " is executing " + o);
                        if (o.getType() == 0) { // READ operation
                            o.setValue(rows[rowNumber].getValue());
                        } else { // WRITE operation
                            rows[rowNumber].setValue(o.getValue());
                        }
                    }
                }

                // Pass transaction to the next queue if not the last thread
                if (outputQueue != null) {
                    outputQueue.put(transaction);
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void serialExecution(List<Transaction> transactions) {
        System.out.println("Serial Execution:");
        for (Transaction t : transactions) {
            for (Operation o : t.getOperations()) {
                System.out.println("T" + t.getId() + " is executing " + o);
                if (o.getType() == 0) { // READ operation
                    o.setValue(rows[o.getRowNumber()].getValue());
                } else { // WRITE operation
                    rows[o.getRowNumber()].setValue(o.getValue());
                }
            }
        }
    }


    public static void main(String[] args) {
        Transaction t1 = new Transaction(1);
        t1.addOperation(new Operation(0, 3, 0));  // Read row 3
        t1.addOperation(new Operation(1, 4, 5));  // Write 5 to row 4
    
        Transaction t2 = new Transaction(2);
        t2.addOperation(new Operation(1, 3, 99)); // Write 99 to row 3
        t2.addOperation(new Operation(0, 14, 0)); // Read row 14
    
        Transaction t3 = new Transaction(3);
        t3.addOperation(new Operation(0, 24, 0)); // Read row 24
        t3.addOperation(new Operation(1, 27, 15)); // Write 15 to row 27
    
        Transaction t4 = new Transaction(4);
        t4.addOperation(new Operation(1, 30, 42)); // Write 42 to row 30
        t4.addOperation(new Operation(0, 40, 0));  // Read row 40
    
        Transaction t5 = new Transaction(5);
        t5.addOperation(new Operation(1, 7, 21));  // Write 21 to row 7
        t5.addOperation(new Operation(0, 8, 0));   // Read row 8

        LinkedList<Transaction> batch = new LinkedList<Transaction>();
        batch.add(t1);
        batch.add(t2);
        batch.add(t3);
        batch.add(t4);
        batch.add(t5);

        Database2 db = new Database2();
        Test test = new Test();
        boolean isSerializable = test.verifySerializability(batch, db);

        if (isSerializable) {
            System.out.println("Concurrent execution is serializable.");
        } else {
            System.out.println("Concurrent execution is NOT serializable.");
        }
    }
}
