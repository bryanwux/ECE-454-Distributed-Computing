import java.util.*;

class TriangleEnumeration{

    private HashMap<Integer, ArrayList<Integer>> graph= null;

    public  TriangleEnumeration(){
        graph = new HashMap<Integer, ArrayList<Integer>>();
    }

    public HashMap<Integer, ArrayList<Integer>> getHash(){
        return graph;
    }

    public void insert(int i, int j){

        //Construct graph
        if(!graph.containsKey(i)){
            graph.put(i, new ArrayList<Integer>());
        }

        graph.get(i).add(j);
    }

    public ArrayList<String> enumerate(){
        ArrayList<String> res = new ArrayList<>();
        for (int key : graph.keySet()){
            for(int i=0; i<graph.get(key).size()-1; i++){
                for(int j=i+1; j<graph.get(key).size(); j++){
                    //These are the neighbours of current vertex
                    int neighbour1=graph.get(key).get(i);
                    int neighbour2=graph.get(key).get(j);
                    //Check if those two neighbours references to each other
                    boolean neighbour1Exist=false;
                    boolean neighbour2Exist=false;
                    if(graph.containsKey(neighbour1)){
                        neighbour1Exist=true;
                    }
                    if(graph.containsKey(neighbour2)){
                        neighbour2Exist=true;
                    }
                    if((neighbour1Exist && graph.get(neighbour1).contains(neighbour2)) || (neighbour2Exist && graph.get(neighbour2).contains(neighbour1))){
                        StringBuilder triangle = new StringBuilder();
                        int max = Math.max(key,Math.max(neighbour1,neighbour2));
                        int min = Math.min(key,Math.min(neighbour1,neighbour2));
                        int mid = (neighbour1+neighbour2+key)-max-min;
                        triangle.append(min).append(" ").append(mid).append(" ").append(max);
                        res.add(triangle.toString());
                    }
                }
            }
        }
        return res;
    }
}