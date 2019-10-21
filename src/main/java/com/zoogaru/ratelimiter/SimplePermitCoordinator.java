package com.zoogaru.ratelimiter;

import org.jgroups.*;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

public class SimplePermitCoordinator extends SimpleCoordinator {

    private String PERMIT_VALUE ;
    private final String PERMIT_KEY = "PK";
    private final String LEADER_ADDRESS_KEY = "leader";
    private CompletableFuture<Boolean> hasFuturePermit ;
    private final AtomicBoolean hasPermitToken = new AtomicBoolean();

    /**
     * Connect to a JGroups cluster
     * @throws Exception
     */
    SimplePermitCoordinator(String nodeName) throws Exception {
        super(nodeName);
    }

    public boolean hasPermit() {
        return hasPermitToken.get();
    }

    public Future<Boolean> requestPermit() {
        hasFuturePermit = new CompletableFuture<>() ;
        if(isLeader()) { //special case
            //locally fetch
            synchronized (state) {
                lookForPermit(state);
                if(hasPermit()) {
                    //Remove the KEY from real leader after giving the state
                    if(state.remove(PERMIT_KEY) != null)
                    {
                        log.info("REMOVED PERMIT_KEY from node : " + channelName);
                    }
                }
            }
        } else {
            getState();
        }
        return hasFuturePermit ;
    }

    private void getState() {
        log.info("GETSTATE CALLED");
        try {
            channel.getState(null, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void initLeader() {
        View view = channel.getView();
        if(view.getMembers().size() == 1) {
            Address leaderAddress = view.getMembers().get(0);
            if (leaderAddress.equals(channel.getAddress())) {
                PERMIT_VALUE = UUID.randomUUID().toString();
                state.put(LEADER_ADDRESS_KEY, leaderAddress.toString());
                state.put(PERMIT_KEY, PERMIT_VALUE);
                log.info("I am the LEADER");
                if(hasPermitToken != null) {
                    hasPermitToken.set(true);
                    log.info("I am the ACTING-LEADER");

                    if(hasFuturePermit != null) {
                        hasFuturePermit.complete(true);
                    }
                }
            } else {
                log.info("I am NOT the LEADER");
                hasPermitToken.set(false); //default
            }
        }
    }

    private void lookForPermit(Map<String, String> aState) {
        String stateLeader = aState.get(LEADER_ADDRESS_KEY) ;
        String leaderToken = aState.get(PERMIT_KEY) ;
        Optional<Address> leaderAddr = getLeaderAddress() ;
        if(leaderToken != null && stateLeader != null) {
            log.info("RECEIVED PERMIT_VALUE " + leaderToken + " at node : " + channelName);
            if(stateLeader.equals(leaderAddr.get().toString())) {
                hasPermitToken.set(true);
                log.info("I am the ACTING-LEADER");
                if(hasFuturePermit != null) {
                    hasFuturePermit.complete(true);
                }
            }
        } else {
            log.info("PERMIT_VALUE not available at node : " + channelName);
            hasPermitToken.set(false);
            //hasFuturePermit.complete(false); //caller should wait
        }
    }

    /**
     * Return the permit after the job is done
     */
    public synchronized void returnPermit() {
        log.info("RETURNING PERMIT_VALUE from node : " + channelName);
        if(PERMIT_VALUE != null) { // leader can have this value so return to state for others
            if (isLeader()) { //I am the leader
                synchronized (state) {
                    state.put(PERMIT_KEY, PERMIT_VALUE);
                }
                log.info("PERMIT_VALUE MADE available at node : " + channelName);
            }
        }
        publishState(); //publish every one that permit is with leader now
        if(PERMIT_VALUE == null) {
            try {
                removePermitFromState();
            } catch (Exception exception) {
                log.info("Exception sending message: " + exception.getMessage());
            }
        }
    }

    @Override
    public void receive(Message message) {
        super.receive(message);
        Map<String, String> stateMap = message.getObject() ;

        if(stateMap != null && isLeader()) {
            String leaderAddress = stateMap.get(LEADER_ADDRESS_KEY);
            String leaderToken = stateMap.get(PERMIT_KEY);
            if (leaderAddress != null && leaderToken != null) { //leader is the only one who is managing
                if(leaderAddress.equals(channelName) && leaderToken.equals(PERMIT_VALUE)) {
                    log.info("PERMIT_VALUE MADE available " + leaderToken + " at node : " + channelName);
                    synchronized(state) {
                        state.put(PERMIT_KEY, PERMIT_VALUE);
                    }
                }
            }
        } else {
            //We may complete the future who is waiting asking the token to leader again
            if(hasFuturePermit != null) {
                hasFuturePermit.complete(false);
            }
        }
    }

    private void removePermitFromState() {
        if(state.remove(PERMIT_KEY) != null) //Remove the KEY from real leader after giving the state
        {
            hasPermitToken.set(false);
            log.info("REMOVED PERMIT_KEY from node : " + channelName);
        }
    }

    @Override
    public void getState(OutputStream output) throws Exception {
        // Serialize into the stream
        synchronized(state) {
            Util.objectToStream(state, new DataOutputStream(output));
            removePermitFromState();
        }
    }

    @Override
    public void setState(InputStream input) {
        log.info("SETSTATE CALLED");
        synchronized(state) {
            try {
                // Deserialize
                state = Util.objectFromStream(new DataInputStream(input));
                lookForPermit(state);
            } catch (Exception e) {
                System.out.println("Error deserialization state!");
            }
        }
    }

}