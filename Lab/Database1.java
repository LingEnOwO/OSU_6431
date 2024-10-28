import java.util.List;
import java.util.LinkedList;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

public class Database1{

    private Row rows[] = new Row[100];

    public Database1(){

	for(int i=0; i<100; i++){
           rows[i] = new Row(i);
        }
    }
     

    public void executeTransactions(List<Transaction> transactions){
        //Here I provide a serial implementation. You need to change it to a concurrent execution.
        ExecutorService executor = Executors.newFixedThreadPool(transactions.size());
        System.out.println("Concurrent execution:");
        for(Transaction t : transactions){
            executor.submit(() -> {
                for(Operation o : t.getOperations()){
                    if(o.getType()==0){
                       rows[o.getRowNumber()].getlock().readLock().lock();
                    }
                    else{
                        rows[o.getRowNumber()].getlock().writeLock().lock();
                    }
                }

                try{
                    for (Operation o : t.getOperations()){
                        System.out.println("T" + t.getId() + " is executing " + o);
                        if(o.getType() == 0){
                            //rows[o.getRowNumber()].setValue(o.getValue());
                            o.setValue(rows[o.getRowNumber()].getValue());
                        }
                        else{
                            rows[o.getRowNumber()].setValue(o.getValue());
                        }
                    }
                }
                finally{
                    for(Operation o : t.getOperations()){
                        if(o.getType()==0){
                           rows[o.getRowNumber()].getlock().readLock().unlock();
                        }
                        else{
                            rows[o.getRowNumber()].getlock().writeLock().unlock();
                        }
                    }
                }

            });
        }
        executor.shutdown();
        while (!executor.isTerminated()) {
            // Wait for all transactions to complete
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

    public Row[] getRowsCopy() {
        Row[] copy = new Row[rows.length];
        for (int i = 0; i < rows.length; i++) {
            copy[i] = new Row(rows[i].getValue());
        }
        return copy;
    }

    public static void main(String []args){
        Transaction t1 = new Transaction(1);
        t1.addOperation(new Operation(0, 1, 0)); 
        t1.addOperation(new Operation(1, 2, 50)); 
        t1.addOperation(new Operation(0, 3, 0)); 
        
        Transaction t2 = new Transaction(2);
        t2.addOperation(new Operation(1, 4, 75)); 
        t2.addOperation(new Operation(0, 1, 0)); 
        t2.addOperation(new Operation(1, 3, 30)); 
    
    
        LinkedList<Transaction> batch = new LinkedList<>();
        batch.add(t1);
        batch.add(t2);
        
        Database1 db = new Database1();
        Test Test = new Test();
        //System.out.println("Concurrent execution:");
        boolean isSerializable = Test.verifySerializability(batch, db);

        if (isSerializable) {
            System.out.println("Concurrent execution is serializable.");
        } else {
            System.out.println("Concurrent execution is NOT serializable.");
        }
    }
}