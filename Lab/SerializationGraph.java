import java.util.*;

public class SerializationGraph {

    private final Map<Integer, Set<Integer>> adjacencyList = new HashMap<>();

    public void addEdge(int from, int to) {
        adjacencyList.computeIfAbsent(from, k -> new HashSet<>()).add(to);
    }

    public boolean hasCycle() {
        Set<Integer> visited = new HashSet<>();
        Set<Integer> recursionStack = new HashSet<>();
        for (Integer node : adjacencyList.keySet()) {
            if (dfsCycleCheck(node, visited, recursionStack)) {
                return true;
            }
        }
        return false;
    }

    private boolean dfsCycleCheck(int node, Set<Integer> visited, Set<Integer> recursionStack) {
        if (recursionStack.contains(node)) {
            return true;
        }
        if (visited.contains(node)) {
            return false;
        }
        visited.add(node);
        recursionStack.add(node);
        for (Integer neighbor : adjacencyList.getOrDefault(node, Collections.emptySet())) {
            if (dfsCycleCheck(neighbor, visited, recursionStack)) {
                return true;
            }
        }
        recursionStack.remove(node);
        return false;
    }

    // Method to print the serialization graph
    public void printGraph() {
        System.out.println("Serialization Graph:");
        for (Map.Entry<Integer, Set<Integer>> entry : adjacencyList.entrySet()) {
            int from = entry.getKey();
            Set<Integer> toNodes = entry.getValue();
            System.out.print("Transaction " + from + " -> ");
            for (int to : toNodes) {
                System.out.print("Transaction " + to + " ");
            }
            System.out.println();
        }
    }

    // Verify serializability using the Serialization Graph approach
    public boolean verifySerializability(List<Operation> operationLog) {
        SerializationGraph graph = new SerializationGraph();

        Map<Integer, Integer> lastWriteTransaction = new HashMap<>();
        Map<Integer, Integer> lastReadTransaction = new HashMap<>();

        for (Operation op : operationLog) {
            int transactionId = op.getTransactionId();
            int row = op.getRowNumber();
            int type = op.getType();

            if (type == 1) { // WRITE operation
                if (lastWriteTransaction.containsKey(row) && lastWriteTransaction.get(row) != transactionId) {
                    graph.addEdge(lastWriteTransaction.get(row), transactionId);
                }
                if (lastReadTransaction.containsKey(row) && lastReadTransaction.get(row) != transactionId) {
                    graph.addEdge(lastReadTransaction.get(row), transactionId);
                }
                lastWriteTransaction.put(row, transactionId);
            } else { // READ operation
                if (lastWriteTransaction.containsKey(row) && lastWriteTransaction.get(row) != transactionId) {
                    graph.addEdge(lastWriteTransaction.get(row), transactionId);
                }
                lastReadTransaction.put(row, transactionId);
            }
        }

        // Print the graph before checking for cycles
        graph.printGraph();

        return !graph.hasCycle(); // True if acyclic, meaning serializable
    }
}
