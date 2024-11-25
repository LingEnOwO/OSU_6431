import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Collections;

public class Test {
    
    // Method to compare two database states
    private boolean compareDatabaseStates(Row[] db1, Row[] db2) {
        for (int i = 0; i < db1.length; i++) {
            if (db1[i].getValue() != db2[i].getValue()) {
                return false;
            }
        }
        return true;
    }

    private void print(List<Transaction> serialTransactions){
        System.out.print("The equivalent serial log: ");
        for (Transaction item : serialTransactions) {
            System.out.print("T" + item.getId() + " ");
        }
        System.out.println();
    }

    // Method to generate all permutations of transactions
    private List<List<Transaction>> generatePermutations(List<Transaction> transactions) {
        List<List<Transaction>> result = new ArrayList<>();
        permute(transactions, 0, result);
        return result;
    }

    // Recursive function to permute transactions
    private void permute(List<Transaction> transactions, int start, List<List<Transaction>> result) {
        if (start == transactions.size() - 1) {
            result.add(new LinkedList<>(transactions));
            return;
        }
        for (int i = start; i < transactions.size(); i++) {
            Collections.swap(transactions, i, start);
            permute(transactions, start + 1, result);
            Collections.swap(transactions, i, start);
        }
    }

    // Test method to verify if concurrent execution is serializable
    public boolean verifySerializability(List<Transaction> transactions, Row[] concurrentState) {
        // Generate all possible serial executions
        List<List<Transaction>> allPermutations = generatePermutations(transactions);

        // Compare the final state from concurrent execution with each serial state
        for (List<Transaction> serialTransactions : allPermutations) {
            Database1 serialDb = new Database1();
            serialDb.serialExecution(serialTransactions);  
            if (compareDatabaseStates(concurrentState, serialDb.getRowsCopy())) {
                print(serialTransactions);
                return true; 
            }
        }
        
        return false; 
    }
}
