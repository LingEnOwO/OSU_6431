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
    public void setId(int id){
        this.id = id;
    }
    public int getId(){
        return id;
    }
    
    public void addOperation(int type, int rowNumber, int value) {
        operations.add(new Operation(id, type, rowNumber, value));
    }

    public List<Operation> getOperations(){
        return this.operations;
    }
} 
