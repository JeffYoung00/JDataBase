package planner;

import java.util.ArrayList;
import java.util.List;

public class Utils {

    public static void dfs(List<List<Integer>> graph, int node, boolean[] visited, List<Integer> component) {
        visited[node] = true;
        component.add(node);
        for (int neighbor : graph.get(node)) {
            if (!visited[neighbor]) {
                dfs(graph, neighbor, visited, component);
            }
        }
    }

    //避免返回空
    public static List<Integer> maxConnectedComponents(List<List<Integer>> graph) {
        int numNodes = graph.size();
        boolean[] visited = new boolean[numNodes];
        List<Integer> ret=new ArrayList<>();
        for (int node = 0; node < numNodes; node++) {
            if (!visited[node]) {
                List<Integer> component = new ArrayList<>();
                dfs(graph, node, visited, component);
                if(ret.size()<component.size()){
                    ret=component;
                }
            }
        }
        return ret;
    }
}
