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
    public boolean verifySerializability(List<Transaction> transactions, Database1 db) {
        // Step 1: Execute concurrent version and capture final state
        Database1 concurrentDb = new Database1();
        concurrentDb.executeTransactions(transactions);
        Row[] concurrentState = concurrentDb.getRowsCopy();

        // Step 2: Generate all possible serial executions
        List<List<Transaction>> allPermutations = generatePermutations(transactions);

        // Step 3: Compare the final state from concurrent execution with each serial state
        for (List<Transaction> serialTransactions : allPermutations) {
            Database1 serialDb = new Database1();
            serialDb.serialExecution(serialTransactions);
            if (compareDatabaseStates(concurrentState, serialDb.getRowsCopy())) {
                return true; // Found an equivalent serial execution
            }
        }
        
        return false; // No matching serial execution found, not serializable
    }

    public boolean verifySerializability(List<Transaction> transactions, Database2 db) {
        // Step 1: Execute concurrent version and capture final state
        Database1 concurrentDb = new Database1();
        concurrentDb.executeTransactions(transactions);
        Row[] concurrentState = concurrentDb.getRowsCopy();

        // Step 2: Generate all possible serial executions
        List<List<Transaction>> allPermutations = generatePermutations(transactions);

        // Step 3: Compare the final state from concurrent execution with each serial state
        for (List<Transaction> serialTransactions : allPermutations) {
            Database1 serialDb = new Database1();
            serialDb.serialExecution(serialTransactions);
            if (compareDatabaseStates(concurrentState, serialDb.getRowsCopy())) {
                return true; // Found an equivalent serial execution
            }
        }
        
        return false; // No matching serial execution found, not serializable
    }
}
