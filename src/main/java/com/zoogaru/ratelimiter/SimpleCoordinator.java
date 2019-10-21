package com.zoogaru.ratelimiter;

import org.jgroups.*;
import org.jgroups.util.Util;

import java.io.*;
import java.util.*;
import java.util.logging.Logger;

public class SimpleCoordinator extends ReceiverAdapter {

    Logger log = Logger.getLogger(getClass().getSimpleName());

    protected String clusterName = getClass().getSimpleName();
    protected final String LEADER_ADDRESS_KEY = "leader";

    // Shared data between the cluster's node
    protected Map<String, String> state = new HashMap<>();

    protected JChannel channel ;
    protected String channelName ;
    protected View lastView ;

    /**
     * Connect to a JGroups cluster
     * @throws Exception
     */
    SimpleCoordinator(String nodeName) throws Exception {

        log.info(getClass().getSimpleName() + " starting");

        // Create the channel
        channel = new JChannel("udp.xml");

        if(nodeName != null && !nodeName.isEmpty()) {
            this.channelName = nodeName;
        } else {
            this.channelName = UUID.randomUUID().toString();
        }
        channel.name(this.channelName);
        channel.setReceiver(this);
        channel.setDiscardOwnMessages(true);
        channel.connect(clusterName);

        log.info("Cluster successfully created : " + clusterName );
        log.info("Node joined : " + this.channelName);

        // Start state transfer
        //channel.getState(null, 0);
    }

    public boolean isLeader() {
        Optional<Address> leaderAddress = getLeaderAddress();
        return leaderAddress != null && leaderAddress.get().toString().equals(channelName);
    }

    @Override
    public void viewAccepted(View newView) {

        if(lastView == null) {
            log.info("New View: ");
            newView.forEach(System.out::println);
        } else {
            List<Address> newMembers = View.newMembers(lastView, newView);
            log.info("New members: ");
            newMembers.forEach(System.out::println);

            List<Address> exMembers = View.leftMembers(lastView, newView);
            log.info("Exited members: ");
            exMembers.forEach(System.out::println);
        }
        initLeader();
        lastView = newView ;
    }

    protected void initLeader() {
        View view = channel.getView();
        if(view.getMembers().size() == 1) {
            Address leaderAddress = view.getMembers().get(0);
            if (leaderAddress.equals(channel.getAddress())) {
                state.put(LEADER_ADDRESS_KEY, leaderAddress.toString());
                log.info("I am the LEADER");
            } else {
                log.info("I am NOT the LEADER");
            }
        }
    }

    public void sendStateToLeader() {
        try {
            Optional<Address> leaderAddress = getLeaderAddress();
            Message message = new Message(leaderAddress.get(), state);
            channel.send(message);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void publishState() {
        try {
            Message message = new Message(null, state);
            channel.send(message);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void receive(Message message) {
        // Print source and dest with message
        Map<String, String> stateMap = message.getObject() ;
        String line = "Message received from: " + message.getSrc() + " to: " + message.getDest() + " -> " + stateMap;
        log.info(line);

        if (message.getDest() == null) {
            log.info("current state count: " + state.size());
        }
    }

    @Override
    public void getState(OutputStream output) throws Exception {
        // Serialize into the stream
        synchronized(state) {
            Util.objectToStream(state, new DataOutputStream(output));
        }
    }

    @Override
    public void setState(InputStream input) {
        synchronized(state) {
            try {
                // Deserialize
                state = Util.objectFromStream(new DataInputStream(input));
            } catch (Exception e) {
                System.out.println("Error deserialization state!");
            }
        }
    }

    public Optional<Address> getLeaderAddress() {
        View view = channel.view();
        return view.getMembers().stream().findFirst();
    }

}