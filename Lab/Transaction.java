import java.util.List;
import java.util.LinkedList;

public class Transaction{
    private LinkedList<Operation> operations;
    // Add id for each transaction
    

    public Transaction(){
        operations = new LinkedList<Operation>();
    }
    
    public void addOperation(Operation o){
        operations.add(o);
    }

    public List<Operation> getOperations(){
        return this.operations;
    }
} 
