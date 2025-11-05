package comp512st.paxos;

// Access to the GCL layer
import comp512.gcl.*;

import comp512.utils.*;
import comp512.utils.FailCheck.FailureType;

// Any other imports that you may need.
import java.io.*;
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

	private final int m_numProcesses; //Number of total player processes.
	private final double m_majorityNum; //Number of processed required to meet majority.
	private final long m_maxTimeout = 500; //1 seconds.
	private int m_proposalSlot; //Next move to propose.
	private int m_deliverSlot; //Next move to deliver.

	private ConcurrentHashMap<Integer, PaxosMoveState> m_moveStateMap = new ConcurrentHashMap<>(); //Stores state for each move number.
	private volatile boolean m_running = true; //False when shutdown is enabled. The volatile keyword allows the change to be instantly seen by threads.

	//Store threads to interrupt on shutdown.
	private Thread m_proposerThread;
	private Thread m_listenerThread;

	GCL m_gcl;

	public Paxos(String p_myProcess, String[] p_allGroupProcesses, Logger p_logger, FailCheck p_failCheck) throws InterruptedException, IOException, UnknownHostException, IllegalArgumentException
	{
		this.m_logger = p_logger;
		
		// Rember to call the failCheck.checkFailure(..) with appropriate arguments throughout your Paxos code to force fail points if necessary.
		this.m_failCheck = p_failCheck;

		// Initialize the GCL communication system as well as anything else you need to.
		this.m_gcl = new GCL(p_myProcess, p_allGroupProcesses, null, p_logger);
		m_logger.info("Initialized GCL for process: " + p_myProcess + " with group: " + Arrays.toString(p_allGroupProcesses));

		this.m_proposalSlot = 1;
		this.m_deliverSlot = 1;
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
			m_moveQueue.add(nextMove);
		}

		while (!m_moveQueue.isEmpty()){
			Thread.sleep(50);
		}

		return;
	}


	//Waits until the map entry at the current move number is not null, then retrieves it.
	public Object acceptTOMsg() throws InterruptedException
	{
		while (m_deliveryMap.get(m_deliverSlot) == null){
			Thread.sleep(500);
		}

		DecidedMove nextMove = m_deliveryMap.get(m_deliverSlot);

		Character returnChar = nextMove.getDecidedMove().getChar();
		int playerNum = nextMove.getPlayerNum();

		m_deliverSlot += 1;

		Object[] return_arr = new Object[2];

		return_arr[0] = playerNum;
		return_arr[1] = returnChar;

		return return_arr;
	}

	// Add any of your own shutdown code into this method.
	public void shutdownPaxos(){
		m_logger.info("Shutdown initiated - stopping new proposals");

		m_running = false;

		// Wait up to 2 seconds for pending moves to complete
		try {
			int maxWait = 20; // 20 * 100ms = 2 seconds
			int waited = 0;
			while (!m_moveQueue.isEmpty() && waited < maxWait) {
				Thread.sleep(100);
				waited++;
			}

			// Additional delay to ensure all CONFIRM messages are processed
			Thread.sleep(500);
			m_logger.info("Shutdown drain period complete");
		} catch (InterruptedException e) {
			m_logger.warning("Shutdown interrupted during drain period");
			Thread.currentThread().interrupt();
		}

		m_gcl.shutdownGCL();
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
				while (m_running || !m_moveQueue.isEmpty()){
					
					if (!(m_moveQueue.isEmpty())){
						PaxosMove nextMove = m_moveQueue.peek();
						
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
							Thread.sleep(50);
						}
					}
					else{
						// Only sleep if we're still running, otherwise exit immediately
						if (m_running) {
							Thread.sleep(100);
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
		moveState.reset(); //Reset the move state if this is a retry.

		float ballotID = generateBallotID(p_moveNum);
		PaxosMessage proposeMessage = new ProposeMessage(p_moveNum, m_playerNum, ballotID);
		moveState.setProposedBallot(ballotID);
		m_gcl.broadcastMsg(proposeMessage);

		m_failCheck.checkFailure(FailureType.AFTERSENDPROPOSE);

		long promiseStart = System.currentTimeMillis();

		PaxosMove chosenMove = p_paxosMove;
		int chosenPlayer = m_playerNum;

		while (System.currentTimeMillis() - promiseStart < m_maxTimeout) {

			//If there was a refuse message received, early terminate.
			if (moveState.getRefuseMessageReceived()){
				return false;
			}
			List<PromiseMessage> promiseList = moveState.getPromiseMessageList();

			if (promiseList.size() >= m_majorityNum) {

				m_failCheck.checkFailure(FailureType.AFTERBECOMINGLEADER);

				PaxosMove previouslyChosenMove = getChosenMove(promiseList);

				//If there was a previously accepted move, add the currently proposed move to the head of the queue. Set the chosen move and chosen player accordingly.
				if (previouslyChosenMove != null){
					m_moveQueue.addFirst(p_paxosMove);
					chosenMove = previouslyChosenMove;
					chosenPlayer = getChosenPlayer(promiseList);
				}

				AcceptMessage acceptMsg = new AcceptMessage(p_moveNum, chosenPlayer, ballotID, chosenMove);
				m_gcl.broadcastMsg(acceptMsg);

				long acceptStart = System.currentTimeMillis();

				while (System.currentTimeMillis() - acceptStart < m_maxTimeout) {
					//If there was a reject message received, early terminate.
					if (moveState.getRejectMessageReceived()){
						return false;
					}
					List<AckMessage> ackList = moveState.getAcceptAckMessageList();

					if (ackList.size() >= m_majorityNum) {

						m_moveQueue.poll();

						m_failCheck.checkFailure(FailureType.AFTERVALUEACCEPT);

						ConfirmMessage confirmMsg = new ConfirmMessage(p_moveNum, chosenPlayer, chosenMove);
						m_gcl.broadcastMsg(confirmMsg);

						
						// ensures the slot is in deliveryMap before we consider it decided
						long confirmStart = System.currentTimeMillis();
						while (m_deliveryMap.get(p_moveNum) == null &&
						       System.currentTimeMillis() - confirmStart < 2000) {
							Thread.sleep(50);
						}

						// Log only after CONFIRM is processed
						if (m_deliveryMap.get(p_moveNum) != null) {
							m_logger.info("Slot " + p_moveNum + " confirmed and added to delivery map");
							return true;
						} else {
							m_logger.warning("Slot " + p_moveNum + " CONFIRM timeout - retrying");
							return false;
						}
					}
					Thread.sleep(50);
				}

				return false;
			}
			
			Thread.sleep(50);  
    	}

		return false;
	}

	//Handles the propose message on the acceptor side.
	private void handleProposeMessage(ProposeMessage p_proposeMessage, String p_senderProcess){

		m_failCheck.checkFailure(FailureType.RECEIVEPROPOSE);

		int moveNum = p_proposeMessage.getMoveNum();
		PaxosMoveState moveState = getMoveState(moveNum);

		float ballotID = p_proposeMessage.getBallotID();

		if (ballotID > moveState.getMaxBallotSeen()){
			moveState.setMaxBallotSeen(ballotID);
			PaxosMove lastAcceptedMove = moveState.getAcceptedMove();
			float lastAcceptedBallot = moveState.getAcceptedBallot();
			int lastAcceptedPlayer = moveState.getLastAcceptedPlayer();
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

		if (ballotID == moveState.getProposedBallot()){
			moveState.setRefuseMessageReceived(true);
		}

	}

	//Handles the promise message on the proposer side.
	private void handlePromiseMessage(PromiseMessage p_promiseMessage, String p_senderProcess){

		m_failCheck.checkFailure(FailureType.AFTERSENDVOTE);

		int moveNum = p_promiseMessage.getMoveNum();
		PaxosMoveState moveState = getMoveState(moveNum);

		float ballotID = p_promiseMessage.getBallotID();

		if (ballotID == moveState.getProposedBallot()){
			List<PromiseMessage> promiseMessageList = moveState.getPromiseMessageList();

			promiseMessageList.add(p_promiseMessage);
		}

	}

	//Handles the accept message on the acceptor side.
	private void handleAcceptMessage(AcceptMessage p_acceptMessage, String p_senderProcess){
		int moveNum = p_acceptMessage.getMoveNum();
		PaxosMoveState moveState = getMoveState(moveNum);

		float ballotID = p_acceptMessage.getBallotID();
		PaxosMove move = p_acceptMessage.getMove();
		int playerNum = p_acceptMessage.getPlayerNum();

		if (ballotID >= moveState.getMaxBallotSeen()){
			moveState.setAcceptedMove(move);
			moveState.setAcceptedBallot(ballotID);
			moveState.setMaxBallotSeen(ballotID);
			moveState.setLastAcceptedPlayer(playerNum);

			AckMessage acceptAckMessage = new AckMessage(moveNum, m_playerNum, ballotID);
			m_gcl.sendMsg(acceptAckMessage, p_senderProcess);
		}

		else{
			RejectMessage rejectMessage = new RejectMessage(moveNum, m_playerNum, ballotID);
			m_gcl.sendMsg(rejectMessage, p_senderProcess);
		}
	}

	//Handles the acceptAck message on the proposer side.
	private void handleAcceptAckMessage(AckMessage p_acceptAckMessage, String p_senderProcess){
		int moveNum = p_acceptAckMessage.getMoveNum();
		PaxosMoveState moveState = getMoveState(moveNum);

		float ballotID = p_acceptAckMessage.getBallotID();

		if (ballotID == moveState.getProposedBallot()){
			List<AckMessage> m_acceptMessageList = moveState.getAcceptAckMessageList();

			m_acceptMessageList.add(p_acceptAckMessage);
		}
	}

	//Handles the reject message on the proposer side.
	private void handleRejectMessage(RejectMessage p_rejectMessage){
		int moveNum = p_rejectMessage.getMoveNum();
		PaxosMoveState moveState = getMoveState(moveNum);

		float ballotID = p_rejectMessage.getBallotID();

		if (ballotID == moveState.getProposedBallot()){
			moveState.setRejectMessageReceived(true);
		}
	}

	//Handles the confirm message on the acceptor side and installs the next move.
	private void handleConfirmMessage(ConfirmMessage p_confirmMessage){
		int moveNum = p_confirmMessage.getMoveNum();
		int playerNum = p_confirmMessage.getPlayerNum();
		PaxosMove confirmedMove = p_confirmMessage.getConfirmedMove();

		DecidedMove decidedMove = new DecidedMove(playerNum, confirmedMove);
		m_deliveryMap.put(moveNum, decidedMove);

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

