import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.*;

/**
 * Created by urey on 2017/3/12.
 */
public class ARSTSimulator {

    // template
    static boolean LOCAL = System.getSecurityManager() == null;
    static boolean TO_FILE = false;
    Scanner sc = new Scanner(System.in);

    // code related
    boolean usedARST = true; // use ARST or not
    int taskNumbers = 5; // task numbers
    int cluster_size = 10000;

    int[] n = {10, 100, 1000, 10000};
    Node[] cluster1 = new Node[10];
    Node[] cluster2 = new Node[100];
    Node[] cluster3 = new Node[1000];
    Node[] cluster4 = new Node[10000];
    List<Node> c4 = new ArrayList<Node>();
    List<Task> tasks = new ArrayList<Task>();
    int p = 5; // process1 methods number
    int q = 5; // process2 methods number

    int nodeTotalCPU = 50;
    int nodeTotalMemory = 50;

    // job related
    int taskPort = 31000;
    int taskMemory = 5;
    int taskCPU = 5;
    int containerPort = 31000;
    int containerMemory = 5;
    int containerCPU = 5;

    Random random = new Random();

    void taskInit(Node node) {
        int taskContainerNumber = random.nextInt(4);

        int taskTotalMemory = 0;
        int taskTotalCPU = 0;
        List<Integer> taskPorts = new ArrayList<Integer>();
        Task task = new Task();
        for(int j = 0; j < taskContainerNumber; ++j) {
            // port
            int portNumber = random.nextInt(65536);
            taskPorts.add(portNumber);
            // memory
            int memory = random.nextInt(10);
            taskTotalMemory += memory;
            // cpu
            int cpu = random.nextInt(10);
            taskTotalCPU += cpu;

            Container container = new Container(portNumber, memory, cpu);
            task.getContainers().add(container);
        }
        task.setPorts(taskPorts);
        task.setMemory(taskTotalMemory);
        task.setCpu(taskTotalCPU);
        node.getNodeTasks().add(task);
    }

    void nodeInit(Node node) {
        // simulation task
        int nodeTaskNumber = random.nextInt(15);
        for(int i = 0; i < nodeTaskNumber; ++i) {
            // init task
            taskInit(node);
        }
    }

    void clusterInit() {
        for(int j = 0; j < cluster_size; ++j) {
            Node node = new Node(nodeTotalCPU, nodeTotalMemory);
            // init node
            nodeInit(node);

            cluster4[j] = node;
            c4.add(cluster4[j]);
        }
        System.out.println("Cluster has " + c4.size() + " nodes");
    }

    void jobInit(int taskNumbers) {
        for(int i = 0; i < taskNumbers; ++i) {
            int taskContainerNumber = 1;
            Container container = new Container(containerPort, containerMemory, containerCPU);
            Task task = new Task();
            task.getContainers().add(container);
            tasks.add(task);
        }
    }

    void run() {

        // init
        clusterInit();
        jobInit(taskNumbers);

        // start time
        long startTime = System.currentTimeMillis();
        System.out.println("start scheduling job...");

        for(int k = 0; k < taskNumbers; ++k) {
            System.out.println("scheduling number " + k + " task");
            List<Node> candidateNodes = new ArrayList<Node>();
            // scheduling process one
            for(int j = 0; j < cluster_size; ++j) {
                if(!PodFitsHostPorts(cluster4[j], tasks.get(k))) {
                    continue;
                }
                if(!PodFitsResources(cluster4[j], tasks.get(k))) {
                    continue;
                }
                if(!MatchNodeSelector(cluster4[j], tasks.get(k))) {
                    continue;
                }
                if(!HostName(cluster4[j], tasks.get(k))) {
                    continue;
                }
                if(!NoDiskConflict(cluster4[j], tasks.get(k))) {
                    continue;
                }
                candidateNodes.add(cluster4[j]);
                if(usedARST == true) {
                    if(candidateNodes.size() == taskNumbers * 100) {
                        break;
                    }
                }
            }
            System.out.println("after process1, there are " + candidateNodes.size()
                    + " nodes left");

            // scheduling process two
            List<HostPriority> hostPriorities = new ArrayList<HostPriority>(candidateNodes.size());
            for(int j = 0; j < candidateNodes.size(); ++j) {
                HostPriority hostPriority = new HostPriority();
                hostPriority.setName(candidateNodes.get(j).getId());
                hostPriorities.add(hostPriority);
            }
            for(int j = 0; j < candidateNodes.size(); ++j) {
                LeastRequestedPriority(candidateNodes, tasks.get(k), hostPriorities);
                BalancedResourceAllocation(candidateNodes, tasks.get(k), hostPriorities);
                SelectorSpreadPriority(candidateNodes, tasks.get(k));
                NodeAffinityPriority(candidateNodes, tasks.get(k));
                DeepLearningMixAllocationPriority(candidateNodes, tasks.get(k));
            }
            System.out.println("after process2, there are " + candidateNodes.size()
                    + " nodes left");

            // ranking
            Collections.sort(candidateNodes, new Comparator<Node>() {
                @Override
                public int compare(Node o1, Node o2) {
                    if(o1.score > o2.score) return 1;
                    else return 0;
                }
            });
            System.out.println("choose " + candidateNodes.get(0).getId() + " node");
            System.out.println("\n");
        }

        // end time
        long endTime = System.currentTimeMillis();
        float seconds = (endTime - startTime) / 1000F;
        System.out.println(Float.toString(seconds) + " seconds.");

        // over
        System.out.println("scheduling job over...");
    }

    // scheduling process1 methods
    boolean PodFitsHostPorts(Node node, Task task) {
        for(int i = 0; i < node.getNodeTasks().size(); ++i) {
            for(int j = 0; j < node.getNodeTasks().get(i).getPorts().size(); ++j) {
                if(task.getPort() == node.getNodeTasks().get(i).getPorts().get(j)) {
                    return false;
                }
            }
        }
        return true;
    }

    boolean PodFitsResources(Node node, Task task) {
        int nodeCurrentTotalCPU = 0;
        int nodeCurrentTotalMemory = 0;
        for(int i = 0; i < node.getNodeTasks().size(); ++i) {
            for(int j = 0; j < node.getNodeTasks().get(i).getContainers().size(); ++j) {
                nodeCurrentTotalCPU += node.getNodeTasks().get(i).getContainers().get(j).getCpu();
                nodeCurrentTotalMemory += node.getNodeTasks().get(i).getContainers().get(j).getMemory();
            }
        }
        int taskCurrentTotalCPU = 0;
        int taskCurrentTotalMemory = 0;
        for(int i = 0; i < task.getContainers().size(); ++i) {
            taskCurrentTotalCPU += task.getContainers().get(i).getCpu();
            taskCurrentTotalMemory += task.getContainers().get(i).getMemory();
        }
        if(taskCurrentTotalCPU > nodeTotalCPU - nodeCurrentTotalCPU &&
                taskCurrentTotalMemory > nodeTotalMemory - nodeCurrentTotalMemory) {
            return false;
        }
        return true;
    }

    boolean MatchNodeSelector(Node node, Task task) {
        return true;
    }

    boolean HostName(Node node, Task task) {
        return true;
    }

    boolean NoDiskConflict(Node node, Task task) {
        return true;
    }

    int calculateScore(int requested, int capacity) {
        if(capacity == 0) {
            return 0;
        }
        if(requested > capacity)  {
            return 0;
        }
        return (((capacity - requested) * 10) / capacity);
    }

    // scheduling process2 methods
    void LeastRequestedPriority(List<Node> nodes, Task task, List<HostPriority> hostPriorities) {

        for(int i = 0; i < nodes.size(); ++i) {
            int totalMemory = 0;
            int totalCPU = 0;
            for(int j = 0; j < nodes.get(i).getNodeTasks().size(); ++j) {
                for(int k = 0; k < nodes.get(i).getNodeTasks().get(j).getContainers().size(); ++k) {
                    totalMemory += nodes.get(i).getNodeTasks().get(j).getContainers().get(k).getMemory();
                    totalCPU += nodes.get(i).getNodeTasks().get(j).getContainers().get(k).getCpu();
                }
            }
            totalMemory += task.getMemory();
            totalCPU += task.getCpu();
            int memoryScore = calculateScore(totalMemory, nodes.get(i).getMemory());
            int cpuScore = calculateScore(totalCPU, nodes.get(i).getCpu());
            int score = ((cpuScore + memoryScore) / 2);
            hostPriorities.get(i).setScore(hostPriorities.get(i).getScore() + score);
        }
    }


    double fractionOfCapacity(int requested, int capacity) {
        if(capacity == 0 ){
            return 1;
        }
        return (double)(requested) / (double)(capacity);
    }
    void BalancedResourceAllocation(List<Node> nodes, Task task, List<HostPriority> hostPriorities) {

        for(int i = 0; i < nodes.size(); ++i) {
            int totalMemory = 0;
            int totalCPU = 0;
            for(int j = 0; j < nodes.get(i).getNodeTasks().size(); ++j) {
                for(int k = 0; k < nodes.get(i).getNodeTasks().get(j).getContainers().size(); ++k) {
                    totalMemory += nodes.get(i).getNodeTasks().get(j).getContainers().get(k).getMemory();
                    totalCPU += nodes.get(i).getNodeTasks().get(j).getContainers().get(k).getCpu();
                }
            }
            totalMemory += task.getMemory();
            totalCPU += task.getCpu();
            double memoryFraction = fractionOfCapacity(totalMemory, nodes.get(i).getMemory());
            double cpuFraction = fractionOfCapacity(totalCPU, nodes.get(i).getCpu());
            int score;
            if(cpuFraction >= 1 || memoryFraction >= 1){
                score = 0;
            } else {
                double diff = Math.abs(cpuFraction - memoryFraction);
                score = (int)(10 - diff*10);
            }
            hostPriorities.get(i).setScore(hostPriorities.get(i).getScore() + score);
        }
    }

    void SelectorSpreadPriority(List<Node> nodes, Task task) {

    }

    void NodeAffinityPriority(List<Node> nodes, Task task) {

    }

    void DeepLearningMixAllocationPriority(List<Node> nodes, Task task) {

    }

    public static void main(String[] args) {
        if (LOCAL) {
            try {
                System.setIn(new FileInputStream("/Users/urey/Projects/IntelliJ_java/ACMJava/input/A-large.in"));
            } catch (Throwable e) {
                LOCAL = false;
            }
        }
        if (TO_FILE) {
            try {
                System.setOut(new PrintStream("/Users/urey/Projects/IntelliJ_java/ACMJava/out/A-large.out"));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }

        ARSTSimulator demo = new ARSTSimulator();
        demo.run();
    }
}

class HostPriority{
    String name;
    double score;

    public HostPriority() {

    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }
}

class Node {
    int memory;
    int cpu;
    double score;
    String name;

    public List<Task> getNodeTasks() {
        return nodeTasks;
    }

    boolean unschedulable;
    String id;

    List<Task> nodeTasks;

    public Node(int memory, int cpu) {
        this.memory = memory;
        this.cpu = cpu;
        this.unschedulable = false;
        UUID uuid = UUID.randomUUID();
        String str = uuid.toString();
        // 去掉"-"符号
        String temp = str.substring(0, 8) + str.substring(9, 13) + str.substring(14, 18) + str.substring(19, 23) + str.substring(24);
        this.id = str+","+temp;

        nodeTasks = new ArrayList<Task>();
    }

    public int getMemory() {
        return memory;
    }

    public int getCpu() {
        return cpu;
    }

    public double getScore() {
        return score;
    }

    public String getName() {
        return name;
    }

    public boolean isUnschedulable() {
        return unschedulable;
    }

    public String getId() {
        return id;
    }
}

class Task{
    int port;
    int memory;
    int cpu;
    List<Container> containers;
    List<Integer> ports;

    public Task() {
        containers = new ArrayList<Container>();
        ports = new ArrayList<Integer>();
    }
    public Task(List ports){
        this.port = port;
        containers = new ArrayList<Container>();
        ports = new ArrayList<Integer>();
    }
    public Task(int port, int memory, int cpu){
        this.port = port;
        this.memory = memory;
        this.cpu = cpu;
        containers = new ArrayList<Container>();
        ports = new ArrayList<Integer>();
    }

    public List<Container> getContainers() {
        return containers;
    }

    public void setContainers(List<Container> containers) {
        this.containers = containers;
    }

    public List<Integer> getPorts() {
        return ports;
    }

    public void setPorts(List<Integer> ports) {
        this.ports = ports;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getMemory() {
        return memory;
    }

    public void setMemory(int memory) {
        this.memory = memory;
    }

    public int getCpu() {
        return cpu;
    }

    public void setCpu(int cpu) {
        this.cpu = cpu;
    }
}

class Container{
    int port;
    int memory;
    int cpu;

    public Container(int port){
        this.port = port;
    }
    public Container(int port, int memory, int cpu){
        this.port = port;
        this.memory = memory;
        this.cpu = cpu;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getMemory() {
        return memory;
    }

    public void setMemory(int memory) {
        this.memory = memory;
    }

    public int getCpu() {
        return cpu;
    }

    public void setCpu(int cpu) {
        this.cpu = cpu;
    }
}

