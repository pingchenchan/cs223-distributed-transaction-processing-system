enum TransactionType {
	Transaction1,
	Transaction2,
	Transaction3,
	Transaction4,
	Transaction5,
	Transaction6,
	Transaction7,
}

enum MessageType {
	TwoPhaseLockPrepare,
	TwoPhaseLockReady,
	UserChain,
	SystemChain,
}

Class Message {
	MessageType mMessageType;
	String mSqlCommand;

	Message(MessageType aMessageType, String aSqlCommand){
		mMessageType = aMessageType;
		mSqlCommand = aSqlCommand;
	}

}

Class Transaction {
	int mChainId;
	int mCurrentHop;
	String mCoordinatorId;

	HistoryTable mHistoryTable;
	TransactionType mTransactionType;

	List<Hop> mHop;

	Transaction(TransactionType aTransactionType) {
		mTransactionType = aTransactionType;
		switch (mTransactionType) {
			case TransactionType.Transaction1:
				Hop hop1 = new Hop();
				Hop hop2 = new Hop();
				...
				mHop.insert(hop1);
				mHop.insert(hop2);
				break;
				...
			default:
				break;
		}
	}

	function peek() {
		return mHop.get(mCurrentHop);
	}

	function pop() {
		mCurrentHop += 1;
	}

	function signCoordinator(String aServerSignature) {
		this.mCoordinatorId = aServerSignature;
	}

	function getCoordinator() {
		return mCoordinatorId;
	}

	function getCurrentHop() {
		return mHop.get(mCurrentHop);
	}
}

Class Hop {
	boolean mCompleted;
	String mMySqlInstructions;
	String mServerSignature; // Not sure if we need this. Just to memorize which server completed the hop.

	function markCompleted(String aServerSignature) {
		mCompleted = true;
		mServerSignature = aServerSignature;
	}
}

Class HistoryTable {

}

Class TransactionQueue {
	Queue mTransactionQueue;
}

Class Server() {
	final String S_SERVER_SIGNATURE = "123456";

	HistoryTable mHistoryTable;
	TransactionQueue mTransactionQueue;

	function provideInterface() {
		/**
		 * ToDo: 
		 * Interface to trigger function such as command line
		 * This function differs from server to server
		 */

		Transaction transaction = new Transaction(TransactionType);
		processFirstHop(transaction);
		forward(transaction);
	}

	function processFirstHop(Transaction aTransaction) {
		aTransaction.signCoordinator(S_SERVER_SIGNATURE);
		executeHop(aTransaction.peek());
		aTransaction.pop();
	}

	function executeHop(Hop aHop) {
		// ToDo: Complete hop job. This function differs from servers to servers, transactions to transactions, hop to hop.
		hop.markCompleted(S_SERVER_SIGNATURE);
	}

	function listen() {
		Message message = socket.listen();
		switch (message.getType()) {
			case MessageType.SystemChain:
				// update local data
				break;
		}
	}

	function extractQueue() {
		Transaction t = TransactionQueue.peek();
		Hop hop = t.getCurrentHop();

		if (executale(hop)) {
			executeHop(hop);
		}

		forward(t);
	}

	function executable(Hop hop) {
		// ToDo: check if executable

		return true;
	}

	function forward(Transaction t) {
		// Check which server to send the transaction
		if (t.getCoordinator().equals(S_SERVER_SIGNATURE)) {
			// This server is the coordinator
		} else {
			new Message = Message(MessageType.UserChain);
			sendToCoordinator(t);
		}
	}

	function createSystemChain(Hop aHop) {
		String sqlCommand = ""; // some update command
		Message message = new Message(MessageType.SystemChain, sqlCommand);
		socket.send(message);
	}

	function main() {
		new Thread(provideInterface);
		new Thread(listen);
		new Thread(extractQueue);
	}
}



Maintain a transaction queue
Maintain a history tables list containing ids to history tables

LOOP{
	Insert all user requests as transactions to the transaction_queue;
	Check message from other programs;
	If backward message{
		If failed -> send message again;
		If successful ->
		Go to the history table of this completed hop;
		Current_hop = Find the next hop in the history table;
		Handle_hop();
	}
	Else if forward message{
		Current_hop = hop to do in the forward message;
		process this hop;
		Send backward message with completion information;
	}
	Else{
		current_transaction = transaction_queue.pop
		If new transaction -> history_tables.append = create history tables for current_transaction
			If new transaction -> Acquire_all_locks();
			Lock failed -> append the transaction to queue
			Lock success -> Loop through the hops{
			Handle_hop();
			If forward to other servers -> break the loop and append it to queue
		}
	}
}

Handle_hop:
	check if this hop can be done in this node;
	If can{
		process this hop;
		If it is the first hop, return to the user;
	}
	Else{
		Forward the hop to the corresponding server with history table;
	}

Acquire_all_locks:
	For all hops in transactions
	If transaction 1-6{
	If read{
	Check if lockBI;
	If lockBI -> fail;
	If not lockBI -> success;
}
If insert{
	Check if lockBR;
	If lockBR -> fail;
	If not lockBR -> success;
}
}
	If transaction 7{
	If read{
	Check if lockAI;
	If lockAI -> fail;
	If not lockAI -> success;
}
If insert{
	Check if lockAR;
	If lockAR -> fail;
	If not lockAR -> success;
}
}
	If any fail -> acquire lock failed;
	If all success -> acquire lock success and acquire all the locks; (T1-6 as A, T7 as B, read as R, insert as I)
