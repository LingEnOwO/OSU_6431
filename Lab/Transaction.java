import java.util.List;
import java.util.LinkedList;

public class Transaction{
    private LinkedList<Operation> operations;
    // Add id for each transaction
    private int id;

    public Transaction(int id){
        operations = new LinkedList<Operation>();
        this.id = id;
    }
    
    public void addOperation(Operation o){
        operations.add(o);
    }

    public List<Operation> getOperations(){
        return this.operations;
    }
} 
