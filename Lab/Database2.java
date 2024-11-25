import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

public class Database2 {

    private Row rows[] = new Row[100];
    private final int NUM_THREADS = 10;
    private final int PARTITION_SIZE = 10;
    private List<BlockingQueue<Transaction>> queues = new LinkedList<>();;
    private List<Operation> operationLog = new ArrayList<>();
    private List<String> executionTrace = new ArrayList<>(); // To store execution trace


    public Database2() {
        for (int i = 0; i < 100; i++) {
            rows[i] = new Row(i);
        }

        for (int i = 0; i < NUM_THREADS; i++) {
            queues.add(new LinkedBlockingQueue<>()); 
        }
    }

    public void executeTransactions(List<Transaction> transactions) {
        // Create and start threads
        //System.out.println("Pipeline Execution:");
        CountDownLatch latch = new CountDownLatch(NUM_THREADS); // Countdown to wait for all threads to complete
        for (int i = 0; i < NUM_THREADS; i++) {
            //final int threadId = i;
            //new Thread(() -> processTransactions(threadId)).start();
            final int threadId = i;
            Thread thread = new Thread(() -> processTransactions(threadId, latch)); // Pass latch to each thread
            thread.start();
        }

        // Enqueue all transactions into the first queue to begin processing
        try {
            for (Transaction t : transactions) {
                for (Operation o : t.getOperations()) {
                    operationLog.add(o); 
                }
                queues.get(0).put(t);
            }

            // Add a termination signal (poison pill) with ID -1
            queues.get(0).put(new Transaction(-1));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // Wait for all threads to complete
        try {
            latch.await(); // Wait until all threads have counted down
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void processTransactions(int threadId, CountDownLatch latch) {
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
                        if (o.getType() == 0) { // READ operation
                            o.setValue(rows[rowNumber].getValue());
                        } else { // WRITE operation
                            rows[rowNumber].setValue(o.getValue());
                        }
                        String action = "T" + transaction.getId() + " is executing " + o;
                        synchronized (executionTrace) {
                            executionTrace.add(action);
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
        } finally {
            latch.countDown(); // Countdown latch after thread completes
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
        Transaction t1 = new Transaction(1);
        t1.addOperation(1, 3, 5);  // Read row 3
        t1.addOperation(1, 4, 5);  // Write 5 to row 4
        t1.addOperation(0, 3, 0); 
        t1.addOperation(1, 4, 50); 
        t1.addOperation(0, 5, 0); 
    
        Transaction t2 = new Transaction(2);
        t2.addOperation(1, 3, 75); 
        t2.addOperation(0, 4, 0); 
        t2.addOperation(1, 5, 30); 
        t2.addOperation(0, 3, 0); // Write 99 to row 3
        t2.addOperation(0, 14, 0); // Read row 14
    
        Transaction t3 = new Transaction(3);
        t3.addOperation(0, 2, 0); 
        t3.addOperation(1, 1, 20); 
        t3.addOperation(0, 4, 0); 
        t3.addOperation(1, 4, 10); // Read row 24
        t3.addOperation(1, 27, 15); // Write 15 to row 27
    
        Transaction t4 = new Transaction(4);
        t4.addOperation(0, 4, 0); // Write 42 to row 30
        t4.addOperation(0, 40, 0);  // Read row 40
    
        Transaction t5 = new Transaction(5);
        t5.addOperation(1, 3, 20);  // Write 21 to row 7
        t5.addOperation(0, 8, 0);   // Read row 8

        LinkedList<Transaction> batch = new LinkedList<Transaction>();
        batch.add(t1);
        batch.add(t2);
        batch.add(t3);
        batch.add(t4);
        batch.add(t5);

        Database2 db = new Database2();
        db.executeTransactions(batch);
        for (String action : db.executionTrace) {
            System.out.println(action);
        }
        Test test = new Test();
        Row[] concurrentState = db.getRowsCopy();
        boolean res = test.verifySerializability(batch, concurrentState);

        if (res){
            System.out.println("The concurrent execution is equivalent to a serial log.");
        }
        else{
            System.out.println("The concurrent execution is NOT equivalent to any serial log.");
        }

        SerializationGraph graph = new SerializationGraph();
        boolean isSerializable = graph.verifySerializability(db.getOperationLog());

        if (isSerializable) {
            System.out.println("Concurrent execution is serializable.");
        } else {
            System.out.println("Concurrent execution is NOT serializable.");
        }
    }
}
