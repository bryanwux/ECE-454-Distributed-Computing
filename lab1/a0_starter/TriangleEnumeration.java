import java.util.*;

class TriangleEnumeration{

    private HashMap<Integer, HashSet<Integer>> graph= null;

    public  TriangleEnumeration(){
        graph = new HashMap<Integer, HashSet<Integer>>();
    }

    public HashMap<Integer, HashSet<Integer>> getHash(){
        return graph;
    }

    public void insert(int i, int j){

        //Construct graph
        if(!graph.containsKey(i)){
            graph.put(i, new HashSet<Integer>());
        }

        graph.get(i).add(j);
    }

    public ArrayList<String> enumerate(){
        ArrayList<String> res = new ArrayList<>();

        for (int graphKey : graph.keySet()) {
//            System.out.println("graphKey: " + graphKey);
            for (int hashSetKey : graph.get(graphKey)) {
//                System.out.println("hashSetKey: " + hashSetKey);
                if (graph.get(hashSetKey) != null) {
                    for (int compareKey : graph.get(hashSetKey)) {
                        if (graph.get(graphKey).contains(compareKey)) {
                            StringBuilder triangle = new StringBuilder();
                            int max = Math.max(graphKey, Math.max(hashSetKey, compareKey));
                            int min = Math.min(graphKey, Math.min(hashSetKey, compareKey));
                            int mid = (graphKey + hashSetKey + compareKey) - max - min;
                            triangle.append(min).append(" ").append(mid).append(" ").append(max);
                            res.add(triangle.toString());
                        }
                    }
                }
            }
        }
        return res;
    }
}