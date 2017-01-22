package jm.app;

import java.io.*;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.LinkedList;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public class Master {

    LinkedList<String> workerList;
    LinkedList<WorkerStatus> proposalACKList;
    LinkedList<WorkerStatus> commitACKList;
    LinkedList<WorkerStatus> rollbackACKList;
    String currentOpId = null;
    int workerNum = 0;
    int delay = 30000;

    void readConf() throws FileNotFoundException {
        Scanner scanner = new Scanner(new File("src/main/resources/conf"));
        while (scanner.hasNextLine()) {
            String workerHostName = scanner.nextLine();
            workerList.add(workerHostName);
            workerNum++;
        }
    }

    void sendProposal() {
        currentOpId = UUID.randomUUID().toString();
        for (final String workerHostName : workerList) {
            Thread thread = new Thread(new Runnable() {
                public void run() {
                    try {
                        // period one: connect the worker
                        Socket socket = new Socket(workerHostName, 20009);
                        socket.setSoTimeout(delay); // wait for 10 seconds
                        PrintStream out = new PrintStream(socket.getOutputStream());
                        out.println(currentOpId);
                        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        String res = in.readLine();
                        if (res == null || "".equals(res)) {
                            proposalACKList.add(new WorkerStatus(workerHostName, "fail"));
                        } else if (res.equals("ack")) {
                            proposalACKList.add(new WorkerStatus(workerHostName, "ack"));
                            System.out.println(workerHostName + " accept the proposal");
                        } else {
                            proposalACKList.add(new WorkerStatus(workerHostName, "fail"));
                        }
                        out.flush();
                        out.close();
                        socket.close();
                    } catch (SocketTimeoutException e) {
                        proposalACKList.add(new WorkerStatus(workerHostName, "fail"));
                        System.out.println("Time out for worker " + workerHostName);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
            thread.start();
        }
    }

    boolean checkProposalACK() {
        if (proposalACKList.size() != workerNum) return false;
        for (WorkerStatus workerStatus : proposalACKList) {
            if (workerStatus.getStatus().equals("fail")) {
                return false;
            }
        }
        return true;
    }

    void sendCommit() {
        for (final String workerHostName : workerList) {
            Thread thread = new Thread(new Runnable() {
                public void run() {
                    try {
                        Socket socket = new Socket(workerHostName, 20009);
                        socket.setSoTimeout(delay);
                        PrintStream out = new PrintStream(socket.getOutputStream());
                        out.println(currentOpId + "/commit");
                        out.flush();
                        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        String res = in.readLine();
                        if (res == null || "".equals(res)) {
                            commitACKList.add(new WorkerStatus(workerHostName, "fail"));
                        } else if (res.equals("ack")) {
                            commitACKList.add(new WorkerStatus(workerHostName, "ack"));
                            System.out.println(workerHostName + " acknowledge the commitment");
                        } else {
                            commitACKList.add(new WorkerStatus(workerHostName, "fail"));
                        }
                        in.close();
                        out.close();
                        socket.close();
                    } catch (SocketTimeoutException e) {
                        commitACKList.add(new WorkerStatus(workerHostName, "fail"));
                        System.out.println("Worker " + workerHostName + " time out when rolling back!");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
            thread.start();
        }
    }

    void sendRollback() {
        for (final String workerHostName : workerList) {
            Thread thread = new Thread(new Runnable() {
                public void run() {
                    try {
                        Socket socket = new Socket(workerHostName, 20009);
                        socket.setSoTimeout(delay);
                        PrintStream out = new PrintStream(socket.getOutputStream());
                        out.println(currentOpId + "/rollback");
                        out.flush();
                        out.close();
                        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        String res = in.readLine();
                        if (res == null || "".equals(res)) {
                            rollbackACKList.add(new WorkerStatus(workerHostName, "fail"));
                        } else if (res.equals("ack")) {
                            rollbackACKList.add(new WorkerStatus(workerHostName, "ack"));
                            System.out.println(workerHostName + " acknowledge the roll back operation");
                        } else {
                            rollbackACKList.add(new WorkerStatus(workerHostName, "fail"));
                        }
                        socket.close();
                    } catch (SocketTimeoutException e) {
                        rollbackACKList.add(new WorkerStatus(workerHostName, "fail"));
                        System.out.println("Worker " + workerHostName + " time out when rolling back!");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
            });
            thread.start();
        }
    }

    boolean checkCommitACK() {
        if (commitACKList.size() != workerNum) return false;
        for (WorkerStatus workerStatus : commitACKList) {
            if (workerStatus.getStatus().equals("fail")) {
                return false;
            }
        }
        return true;
    }

    boolean checkRollbackACK() {
        if (rollbackACKList.size() != workerNum) return false;
        for (WorkerStatus workerStatus : rollbackACKList) {
            if (workerStatus.getStatus().equals("fail")) {
                return false;
            }
        }
        return true;
    }

    public Master() {
        workerList = new LinkedList<String>();
        proposalACKList = new LinkedList<WorkerStatus>();
        commitACKList = new LinkedList<WorkerStatus>();
        rollbackACKList = new LinkedList<WorkerStatus>();
    }

    /**
     * @return isTimeOut
     */
    boolean waitForWorker() {
        for (int i = 0; i < 3; i++) {
            if (proposalACKList.size() != workerNum) {
                try {
                    Thread.sleep(delay/50);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                return false;
            }
        }
        return true;
    }

    void waitForSeconds(){
        try{
            Thread.sleep(delay/10);
        }catch (InterruptedException e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Master master = new Master();
        try {
            master.readConf();
            for (int i = 0; i < 5; i++) {
                master.proposalACKList.clear();
                master.commitACKList.clear();
                master.rollbackACKList.clear();
                master.sendProposal();
                boolean isTimeOut = master.waitForWorker();
                if (isTimeOut == true) {
                    master.sendRollback();
                    while(!master.checkRollbackACK()){
                        master.waitForSeconds();
                    }
                } else if (master.checkProposalACK() == true) {
                    master.sendCommit();
                    while(!master.checkCommitACK()){
                        master.waitForSeconds();
                    }
                } else {
                    master.sendRollback();
                    while(!master.checkRollbackACK()){
                        master.waitForSeconds();
                    }
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
