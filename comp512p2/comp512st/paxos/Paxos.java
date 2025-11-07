package comp512st.paxos;

// Access to the GCL layer
import comp512.gcl.*;

import comp512.utils.*;
import comp512.utils.FailCheck.FailureType;

// Any other imports that you may need.
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.logging.*;
import java.net.UnknownHostException;


// ANY OTHER classes, etc., that you add must be private to this package and not visible to the application layer.

// extend / implement whatever interface, etc. as required.
// NO OTHER public members / methods allowed. broadcastTOMsg, acceptTOMsg, and shutdownPaxos must be the only visible methods to the application layer.
//		You should also not change the signature of these methods (arguments and return value) other aspects maybe changed with reasonable design needs.

class DecidedMove {
    private final int m_playerNum;
    private final PaxosMove m_decidedMove;

    DecidedMove(int p_playerNum, PaxosMove p_decidedMove) {
        m_playerNum = p_playerNum;
        m_decidedMove = p_decidedMove;
    }

    int getPlayerNum() {
        return m_playerNum;
    }

    PaxosMove getDecidedMove() {
        return m_decidedMove;
    }
}

public class Paxos
{
	FailCheck m_failCheck;
	Logger m_logger;
	private int m_playerNum; 

	private ConcurrentLinkedDeque<PaxosMove> m_moveQueue = new ConcurrentLinkedDeque<>(); //Queue that stores the moves that are yet to be broadcast.
	private ConcurrentHashMap<Integer, DecidedMove> m_deliveryMap = new ConcurrentHashMap<>(); //Maps slot numbers to <proposer process, decided move> at that slot.

	private final int m_numProcesses;
	private final double m_majorityNum; 
	private final long m_maxTimeout = 500;
	private static final long SHUTDOWN_QUIET_PERIOD_MS = 4000;
	private static final long SHUTDOWN_MAX_WAIT_MS = 15000;
	private int m_proposalSlot; //Next move to propose.
	private int m_deliverSlot; //Next move to deliver.

	private ConcurrentHashMap<Integer, PaxosMoveState> m_moveStateMap = new ConcurrentHashMap<>(); //Stores state for each move number.
	private volatile boolean m_running = true; 
	private volatile boolean m_shutdownRequested = false; //True once shutdown has been requested.
	private volatile long m_lastDeliveryTime; 
	private final Object m_queueMonitor = new Object();
	private final Object m_deliveryMonitor = new Object();

	//Store threads to interrupt on shutdown.
	private Thread m_proposerThread;
	private Thread m_listenerThread;

	GCL m_gcl;

	public Paxos(String p_myProcess, String[] p_allGroupProcesses, Logger p_logger, FailCheck p_failCheck) throws InterruptedException, IOException, UnknownHostException, IllegalArgumentException
	{
		this.m_logger = p_logger;
		
		
		this.m_failCheck = p_failCheck;

		// Initialize the GCL communication system as well as anything else you need to.
		this.m_gcl = new GCL(p_myProcess, p_allGroupProcesses, null, p_logger);
		m_logger.info("Initialized GCL for process: " + p_myProcess + " with group: " + Arrays.toString(p_allGroupProcesses));

		this.m_proposalSlot = 1;
		this.m_deliverSlot = 1;
		this.m_lastDeliveryTime = System.currentTimeMillis();
		m_numProcesses = p_allGroupProcesses.length;
		m_majorityNum = Math.ceil(m_numProcesses / 2.0);
		m_playerNum = 0;
		
		for (int i = 0; i < p_allGroupProcesses.length; i++){
			if (p_allGroupProcesses[i].equals(p_myProcess)){
				m_playerNum = i+1;
				break;
			}
		}

		startListenerThread();
		startProposerThread();
	}

	public void broadcastTOMsg(Object val) throws InterruptedException
	{
		Object[] moveArr = (Object[]) val; 

		int playerNum = (Integer) moveArr[0];
		PaxosMove nextMove = PaxosMove.fromChar((Character) moveArr[1]);

		if (playerNum == m_playerNum){
			synchronized (m_queueMonitor){
				m_moveQueue.add(nextMove);
				m_queueMonitor.notifyAll();
			}
		}

		synchronized (m_queueMonitor){
			while (!m_moveQueue.isEmpty()){
				m_queueMonitor.wait();
			}
		}

		return;
	}


	//Waits until the map entry at the current move number is not null, then retrieves it.
	public Object acceptTOMsg() throws InterruptedException
	{
		DecidedMove nextMove;
		synchronized (m_deliveryMonitor){
			while ((nextMove = m_deliveryMap.get(m_deliverSlot)) == null){
				m_deliveryMonitor.wait();
			}
		}

		Character returnChar = nextMove.getDecidedMove().getChar();
		int playerNum = nextMove.getPlayerNum();

		m_deliverSlot += 1;

		Object[] return_arr = new Object[2];

		return_arr[0] = playerNum;
		return_arr[1] = returnChar;

		return return_arr;
	}


	public void shutdownPaxos(){
		m_logger.info("Shutdown initiated - stopping new proposals");

		m_shutdownRequested = true;
		synchronized (m_queueMonitor){
			m_queueMonitor.notifyAll();
		}
		synchronized (m_deliveryMonitor){
			m_deliveryMonitor.notifyAll();
		}

		// Wait up to 2 seconds for pending moves to complete
		try {
			long queueDeadline = System.currentTimeMillis() + 2000;
			synchronized (m_queueMonitor){
				while (!m_moveQueue.isEmpty() && System.currentTimeMillis() < queueDeadline) {
					long waitTime = queueDeadline - System.currentTimeMillis();
					if (waitTime <= 0) {
						break;
					}
					m_queueMonitor.wait(waitTime);
				}
			}

			// Additional delay to ensure all CONFIRM messages are processed
			synchronized (m_deliveryMonitor){
				long quietStart = System.currentTimeMillis();
				while ((System.currentTimeMillis() - m_lastDeliveryTime) < SHUTDOWN_QUIET_PERIOD_MS &&
				       (System.currentTimeMillis() - quietStart) < SHUTDOWN_MAX_WAIT_MS) {
					long sinceLastDelivery = System.currentTimeMillis() - m_lastDeliveryTime;
					long waitForQuiet = SHUTDOWN_QUIET_PERIOD_MS - sinceLastDelivery;
					long remainingBudget = SHUTDOWN_MAX_WAIT_MS - (System.currentTimeMillis() - quietStart);
					long waitTime = Math.min(waitForQuiet, remainingBudget);
					if (waitTime <= 0) {
						break;
					}
					m_deliveryMonitor.wait(waitTime);
				}
			}
			m_logger.info("Shutdown drain period complete");
		} catch (InterruptedException e) {
			m_logger.warning("Shutdown interrupted during drain period");
			Thread.currentThread().interrupt();
		}

		m_gcl.shutdownGCL();
		m_running = false;

		if (m_listenerThread != null) {
			m_listenerThread.interrupt();
			try {
				m_listenerThread.join(1000);
			} catch (InterruptedException e) {
				m_logger.warning("Interrupted while waiting for listener thread to terminate");
				Thread.currentThread().interrupt();
			}
		}

		if (m_proposerThread != null) {
			try {
				m_proposerThread.join(1000);
			} catch (InterruptedException e) {
				m_logger.warning("Interrupted while waiting for proposer thread to terminate");
				Thread.currentThread().interrupt();
			}
		}

		m_logger.info("Paxos shutdown complete");
	}

	private void startListenerThread(){
		Thread listenerThread = new Thread(() -> {
			try{
				while (m_running){
					GCMessage gcMsg = m_gcl.readGCMessage();
					Object msg = gcMsg.val;

					if (msg instanceof ProposeMessage) {
						handleProposeMessage((ProposeMessage) msg, gcMsg.senderProcess);
					} 
					else if (msg instanceof PromiseMessage) {
						handlePromiseMessage((PromiseMessage) msg, gcMsg.senderProcess);
					} 
					else if (msg instanceof RefuseMessage) {
						handleRefuseMessage((RefuseMessage) msg);
					} 
					else if (msg instanceof AcceptMessage) {
						handleAcceptMessage((AcceptMessage) msg, gcMsg.senderProcess);
					} 
					else if (msg instanceof AckMessage) {
						handleAcceptAckMessage((AckMessage) msg, gcMsg.senderProcess);
					} 
					else if (msg instanceof RejectMessage) {
						handleRejectMessage((RejectMessage) msg);
					} 
					else if (msg instanceof ConfirmMessage) {
						handleConfirmMessage((ConfirmMessage) msg);
					}
				}
			}
			catch (InterruptedException e){
				Thread.currentThread().interrupt();
			}
		});

		m_listenerThread = listenerThread;
		listenerThread.start();
	}

	private void startProposerThread() throws InterruptedException{
		Thread proposerThread = new Thread(() -> {
			try{
				while (true){
					PaxosMove nextMove;
					boolean shouldExit = false;
					synchronized (m_queueMonitor){
						while ((nextMove = m_moveQueue.peek()) == null){
							if (m_shutdownRequested){
								shouldExit = true;
								break;
							}
							m_queueMonitor.wait();
						}
					}

					if (shouldExit){
						break;
					}

					m_logger.info("Attempting to propose move: " + nextMove + " from player " + m_playerNum);

					int slotNum = m_proposalSlot;

					if (m_deliveryMap.get(slotNum) != null){
						m_logger.info("Slot " + slotNum + " has already been decided.");
						m_proposalSlot += 1;
						continue;
					}

					if (runPaxos(nextMove, slotNum)){
						m_proposalSlot += 1;
					}
					else{
						synchronized (m_queueMonitor){
							if (!m_shutdownRequested){
								m_queueMonitor.wait(m_maxTimeout);
							}
						}
					}
				}
				m_logger.info("Proposer thread exiting cleanly");
			}
			catch(InterruptedException e){
				m_logger.info("Proposer thread interrupted");
				Thread.currentThread().interrupt();
			}
		});
		
		m_proposerThread = proposerThread;
		proposerThread.start();
	}

	//Starts paxos round with given move and random ballot ID. Returns true if successful installment was made by this process.
	private boolean runPaxos(PaxosMove p_paxosMove, Integer p_moveNum) throws InterruptedException{

		PaxosMoveState moveState = getMoveState(p_moveNum);
		synchronized (moveState){
			 //Reset the move state if this is a retry.
			moveState.reset();
		}

		float ballotID = generateBallotID(p_moveNum);
		PaxosMessage proposeMessage = new ProposeMessage(p_moveNum, m_playerNum, ballotID);
		synchronized (moveState){
			moveState.setProposedBallot(ballotID);
		}
		m_gcl.broadcastMsg(proposeMessage);

		m_failCheck.checkFailure(FailureType.AFTERSENDPROPOSE);

		long promiseDeadline = System.currentTimeMillis() + m_maxTimeout;

		PaxosMove chosenMove = p_paxosMove;
		int chosenPlayer = m_playerNum;

		while (true) {
			synchronized (moveState){
				if (moveState.getRefuseMessageReceived()){
					return false;
				}
				if (moveState.getPromiseMessageList().size() >= m_majorityNum) {
					break;
				}
				if (m_shutdownRequested){
					return false;
				}
				long waitTime = promiseDeadline - System.currentTimeMillis();
				if (waitTime <= 0){
					return false;
				}
				moveState.wait(waitTime);
			}
		}

		List<PromiseMessage> promiseList;
		synchronized (moveState){
			promiseList = new ArrayList<>(moveState.getPromiseMessageList());
		}

		m_failCheck.checkFailure(FailureType.AFTERBECOMINGLEADER);

		PaxosMove previouslyChosenMove = getChosenMove(promiseList);

		//If there was a previously accepted move, add the currently proposed move to the head of the queue. Set the chosen move and chosen player accordingly.
		if (previouslyChosenMove != null){
			synchronized (m_queueMonitor){
				m_moveQueue.addFirst(p_paxosMove);
				m_queueMonitor.notifyAll();
			}
			chosenMove = previouslyChosenMove;
			chosenPlayer = getChosenPlayer(promiseList);
		}

		AcceptMessage acceptMsg = new AcceptMessage(p_moveNum, chosenPlayer, ballotID, chosenMove);
		m_gcl.broadcastMsg(acceptMsg);

		long acceptDeadline = System.currentTimeMillis() + m_maxTimeout;

		while (true) {
			synchronized (moveState){
				if (moveState.getRejectMessageReceived()){
					return false;
				}
				if (moveState.getAcceptAckMessageList().size() >= m_majorityNum) {
					break;
				}
				if (m_shutdownRequested){
					return false;
				}
				long waitTime = acceptDeadline - System.currentTimeMillis();
				if (waitTime <= 0){
					return false;
				}
				moveState.wait(waitTime);
			}
		}

		synchronized (m_queueMonitor){
			m_moveQueue.poll();
			m_queueMonitor.notifyAll();
		}

		m_failCheck.checkFailure(FailureType.AFTERVALUEACCEPT);

		ConfirmMessage confirmMsg = new ConfirmMessage(p_moveNum, chosenPlayer, chosenMove);
		m_gcl.broadcastMsg(confirmMsg);

		long confirmDeadline = System.currentTimeMillis() + 2000;
		synchronized (m_deliveryMonitor){
			while (m_deliveryMap.get(p_moveNum) == null && System.currentTimeMillis() < confirmDeadline) {
				long waitTime = confirmDeadline - System.currentTimeMillis();
				if (waitTime <= 0){
					break;
				}
				if (m_shutdownRequested){
					return false;
				}
				m_deliveryMonitor.wait(waitTime);
			}
		}

		if (m_deliveryMap.get(p_moveNum) != null) {
			m_logger.info("Slot " + p_moveNum + " confirmed and added to delivery map");
			return true;
		} else {
			m_logger.warning("Slot " + p_moveNum + " CONFIRM timeout - retrying");
			return false;
		}

	}

	//Handles the propose message on the acceptor side.
	private void handleProposeMessage(ProposeMessage p_proposeMessage, String p_senderProcess){

		m_failCheck.checkFailure(FailureType.RECEIVEPROPOSE);

		int moveNum = p_proposeMessage.getMoveNum();
		PaxosMoveState moveState = getMoveState(moveNum);

		float ballotID = p_proposeMessage.getBallotID();

		boolean sendPromise;
		PaxosMove lastAcceptedMove;
		float lastAcceptedBallot;
		int lastAcceptedPlayer;

		synchronized (moveState){
			if (ballotID > moveState.getMaxBallotSeen()){
				moveState.setMaxBallotSeen(ballotID);
				lastAcceptedMove = moveState.getAcceptedMove();
				lastAcceptedBallot = moveState.getAcceptedBallot();
				lastAcceptedPlayer = moveState.getLastAcceptedPlayer();
				sendPromise = true;
			}
			else{
				sendPromise = false;
				lastAcceptedMove = null;
				lastAcceptedBallot = moveState.getMaxBallotSeen();
				lastAcceptedPlayer = m_playerNum;
			}
		}

		if (sendPromise){
			PromiseMessage promiseMessage = new PromiseMessage(moveNum, m_playerNum, ballotID, lastAcceptedPlayer, lastAcceptedMove, lastAcceptedBallot);
			m_gcl.sendMsg(promiseMessage, p_senderProcess);
		}
		else{
			RefuseMessage refuseMessage = new RefuseMessage(moveNum, m_playerNum, ballotID);
			m_gcl.sendMsg(refuseMessage, p_senderProcess);
		}

		m_failCheck.checkFailure(FailureType.AFTERSENDVOTE);
	}

	//Handles refuse message on the proposer side.
	private void handleRefuseMessage(RefuseMessage p_refuseMessage){

		m_failCheck.checkFailure(FailureType.AFTERSENDVOTE);

		int moveNum = p_refuseMessage.getMoveNum();
		PaxosMoveState moveState = getMoveState(moveNum);

		float ballotID = p_refuseMessage.getBallotID();

		synchronized (moveState){
			if (ballotID == moveState.getProposedBallot()){
				moveState.setRefuseMessageReceived(true);
				moveState.notifyAll();
			}
		}

	}

	//Handles the promise message on the proposer side.
	private void handlePromiseMessage(PromiseMessage p_promiseMessage, String p_senderProcess){

		m_failCheck.checkFailure(FailureType.AFTERSENDVOTE);

		int moveNum = p_promiseMessage.getMoveNum();
		PaxosMoveState moveState = getMoveState(moveNum);

		float ballotID = p_promiseMessage.getBallotID();

		synchronized (moveState){
			if (ballotID == moveState.getProposedBallot()){
				List<PromiseMessage> promiseMessageList = moveState.getPromiseMessageList();
				promiseMessageList.add(p_promiseMessage);
				moveState.notifyAll();
			}
		}

	}

	//Handles the accept message on the acceptor side.
	private void handleAcceptMessage(AcceptMessage p_acceptMessage, String p_senderProcess){
		int moveNum = p_acceptMessage.getMoveNum();
		PaxosMoveState moveState = getMoveState(moveNum);

		float ballotID = p_acceptMessage.getBallotID();
		PaxosMove move = p_acceptMessage.getMove();
		int playerNum = p_acceptMessage.getPlayerNum();

		boolean sendAck;
		synchronized (moveState){
			if (ballotID >= moveState.getMaxBallotSeen()){
				moveState.setAcceptedMove(move);
				moveState.setAcceptedBallot(ballotID);
				moveState.setMaxBallotSeen(ballotID);
				moveState.setLastAcceptedPlayer(playerNum);
				sendAck = true;
			} else {
				sendAck = false;
			}
		}

		if (sendAck){
			AckMessage acceptAckMessage = new AckMessage(moveNum, m_playerNum, ballotID);
			m_gcl.sendMsg(acceptAckMessage, p_senderProcess);
		} else {
			RejectMessage rejectMessage = new RejectMessage(moveNum, m_playerNum, ballotID);
			m_gcl.sendMsg(rejectMessage, p_senderProcess);
		}
	}

	//Handles the acceptAck message on the proposer side.
	private void handleAcceptAckMessage(AckMessage p_acceptAckMessage, String p_senderProcess){
		int moveNum = p_acceptAckMessage.getMoveNum();
		PaxosMoveState moveState = getMoveState(moveNum);

		float ballotID = p_acceptAckMessage.getBallotID();

		synchronized (moveState){
			if (ballotID == moveState.getProposedBallot()){
				List<AckMessage> m_acceptMessageList = moveState.getAcceptAckMessageList();
				m_acceptMessageList.add(p_acceptAckMessage);
				moveState.notifyAll();
			}
		}
	}

	//Handles the reject message on the proposer side.
	private void handleRejectMessage(RejectMessage p_rejectMessage){
		int moveNum = p_rejectMessage.getMoveNum();
		PaxosMoveState moveState = getMoveState(moveNum);

		float ballotID = p_rejectMessage.getBallotID();

		synchronized (moveState){
			if (ballotID == moveState.getProposedBallot()){
				moveState.setRejectMessageReceived(true);
				moveState.notifyAll();
			}
		}
	}

	//Handles the confirm message on the acceptor side and installs the next move.
	private void handleConfirmMessage(ConfirmMessage p_confirmMessage){
		int moveNum = p_confirmMessage.getMoveNum();
		int playerNum = p_confirmMessage.getPlayerNum();
		PaxosMove confirmedMove = p_confirmMessage.getConfirmedMove();

		synchronized (m_deliveryMonitor){
			DecidedMove decidedMove = new DecidedMove(playerNum, confirmedMove);
			m_deliveryMap.put(moveNum, decidedMove);
			m_lastDeliveryTime = System.currentTimeMillis();
			m_deliveryMonitor.notifyAll();
		}
		m_logger.info("Slot " + moveNum + " has been decided by process " + playerNum);
	}

	//Generates a random ballotID based on the current moveNum from a U(0,1) distribution, rounded to 2 decimal places. I.e. if p_moveNum = 2, then this will output 2.31, 2.93, etc.
	private float generateBallotID(Integer p_moveNum){
		Random rand = new Random();
		float fractional = rand.nextFloat();
		float offset = (0.001f) * m_playerNum; //Slight offset to prevent duplicate ballotID's.
		return p_moveNum + fractional + offset;
	}

	//Return the move state associated with a move number, and creates it if it doesn't exist.
	private PaxosMoveState getMoveState(int p_moveNum){
		
		PaxosMoveState moveState = m_moveStateMap.get(p_moveNum);

		if (moveState != null){
			return moveState;
		}

		else{
			m_moveStateMap.put(p_moveNum, new PaxosMoveState());
			return m_moveStateMap.get(p_moveNum);
		}
	}

	//Returns the previously accepted move with the highest ballot ID from the promise messages received. Returns null if there were no previous moves accepted.
	private PaxosMove getChosenMove(List<PromiseMessage> p_promiseSet){
		PaxosMove chosenMove = null;
		float maxBallot = -1;

		for (PromiseMessage promiseMessage: p_promiseSet){
			float acceptedBallot = promiseMessage.getAcceptedBallot();
			PaxosMove acceptedMove = promiseMessage.getAcceptedMove();

			if (acceptedMove != null && acceptedBallot > maxBallot){
				chosenMove = acceptedMove;
				maxBallot = acceptedBallot;
			}
		} 

		return chosenMove;
	}

	//Returns the last chosen player from promise messages, i.e. the player that proposed the highest ballot ID.
	private int getChosenPlayer(List<PromiseMessage> p_promiseSet){
		int chosenPlayer = -1;
		float maxBallot = -1;

		for (PromiseMessage promiseMessage: p_promiseSet){
			float acceptedBallot = promiseMessage.getAcceptedBallot();
			PaxosMove acceptedMove = promiseMessage.getAcceptedMove();

			if (acceptedMove != null && acceptedBallot > maxBallot){
				chosenPlayer = promiseMessage.getLastAcceptedPlayer(); // Get from promise
				maxBallot = acceptedBallot;
			}
		} 

		return chosenPlayer;
	}

}
