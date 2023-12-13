from datetime import datetime
import sqlite3
from sqlite import *
import uuid
from message import *
from collections import defaultdict


class Hop:
    VALID_STATUSES = {"Active", "Completed", "Failed"}

    def __init__(self, transaction_id, hop_id, data):
        self.transaction_id = transaction_id
        self.hop_id = hop_id
        self.status = "Active"
        self.start_time = datetime.now()
        self.end_time = None
        self.data = data
        self.websocket_tracker = (
            TimeTracker()
        )  # [(send_timestep, receive_timestep), ...]
        self.queue_tracker = TimeTracker()
        self.hop_executor_server = None

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        if value in self.VALID_STATUSES:
            self._status = value
        else:
            raise ValueError(
                f"Status must be one of {self.VALID_STATUSES}, not '{value}'"
            )

    # Method to call when a hop starts processing.
    def start_processing(self):
        self.start_time = datetime.now()

    def end_processing(self):
        self.end_time = datetime.now()

    # Methods specific to websocket transmission
    def start_websocket_transmission(self, timestamp=None):
        self.websocket_tracker.start_event(timestamp)

    def end_websocket_transmission(self, timestamp=None):
        self.websocket_tracker.end_event(timestamp)

    # Methods specific to queue wait time
    def start_queue_wait(self, timestamp=None):
        self.queue_tracker.start_event(timestamp)

    def end_queue_wait(self, timestamp=None):
        self.queue_tracker.end_event(timestamp)

    def concat_queue_wait(self, other):
        self.queue_tracker.concat(other)

    # Aggregate data access methods
    def get_total_websocket_transmission_time(self):
        return self.websocket_tracker.get_total_time()

    def get_total_queue_wait_time(self):
        return self.queue_tracker.get_total_time()

    def get_websocket_transmission_count(self):
        return self.websocket_tracker.get_event_count()

    def get_queue_wait_count(self):
        return self.queue_tracker.get_event_count()


class Transaction:
    def __init__(self, transaction_id, transaction_type, total_hops):
        self.transaction_id = transaction_id
        self.transaction_type = transaction_type
        self.status = "Active"
        self.start_time = datetime.now()
        self.end_time = None
        self.hops = {}  # {1: Hop, 2: Hop, ...}
        self.total_hops = total_hops
        self.last_completed_hop = 0

    def end_processing(self):
        self.end_time = datetime.now()


class HistoryTable:
    _instance = None
    _is_initialized = False

    def __new__(cls):
        if cls._instance is None:  # Singleton pattern
            cls._instance = super(HistoryTable, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not self._is_initialized:
            self.transactions = {}
            self.current_server_type = None
            self._is_initialized = True

    def add_transaction(self, transaction_type, total_hops) -> str:
        transaction_id = str(uuid.uuid4())  # Generate a random transaction ID
        self.transactions[transaction_id] = Transaction(
            transaction_id, transaction_type, total_hops
        )
        # self.generate_corresponding_hop(transaction_id, data)

        return transaction_id

    def generate_corresponding_hop(self, transaction_id, data) -> Hop:
        transaction_type = self.transactions.get(transaction_id).transaction_type
        if transaction_type in [
            transaction_type.T1,
            transaction_type.T2,
            transaction_type.T5,
            transaction_type.T6,
        ]:
            self.transactions[transaction_id].hops[1] = Hop(
                transaction_id=transaction_id, hop_id=1, data=data
            )  # hop_id start from 1
        if transaction_type in [transaction_type.T3, transaction_type.T4,transaction_type.T7]:
            self.transactions[transaction_id].hops[1] = Hop(
                transaction_id=transaction_id, hop_id=1, data=data
            )
            self.transactions[transaction_id].hops[2] = Hop(
                transaction_id=transaction_id, hop_id=2, data=data
            )
        return self.transactions[transaction_id].hops

    def complete_transaction_hop(self, hop, hop_executor_server):
        transaction = self.transactions.get(hop.transaction_id)
        if hop and hop.status == "Active":
            hop.status = "Completed"
            hop.end_processing()
            hop.hop_executor_server = hop_executor_server
            transaction.last_completed_hop = hop.hop_id
            if transaction.last_completed_hop == transaction.total_hops:
                transaction.status = "Completed"
                transaction.end_processing()
                return True
            else:
                return False
        elif hop and hop.status == "Completed":
            pass
            # print('Hop has already been completed.', 'Hop ID:', hop.hop_id,'transaction type:', transaction.transaction_type)
        else:
            print(
                f"Unable to complete hop: {hop.hop_id} not found or status id fail. Hop status is : {hop.status }"
            )

    def find_next_hop(self, transaction_id):
        transaction = self.transactions.get(transaction_id)
        if transaction:
            for hop_id in range(
                transaction.last_completed_hop + 1, transaction.total_hops + 1
            ):
                hop = transaction.hops.get(hop_id)
                if hop and hop.status == "Active":
                    return hop
            return None
        else:
            print(f"789 Transaction {transaction_id} not found.")

    async def write_transaction_log(self, each_transaction=True, filename=None):
        """Write the transaction log to a file."""

        total_latency = 0
        total_transactions = 0
        total_first_hop_latency_by_type = defaultdict(int)
        total_second_hop_latency_by_type = defaultdict(int)
        first_hop_count_by_type = defaultdict(int)
        second_hop_count_by_type = defaultdict(int)

        total_latency_by_type = defaultdict(float)
        count_by_type = defaultdict(int)

        queue_wait_time_by_hop = defaultdict(lambda: defaultdict(int))
        websocket_transmission_time_by_hop = defaultdict(lambda: defaultdict(int))
        queue_wait_count_by_hop = defaultdict(lambda: defaultdict(int))
        queue_wait_count_by_hop_2 = defaultdict(lambda: defaultdict(int))
        websocket_transmission_count_by_hop = defaultdict(lambda: defaultdict(int))
        websocket_transmission_count_by_hop_2 = defaultdict(lambda: defaultdict(int))

        with open(f"./log/{filename}-{self.current_server_type}.transaction_log.txt", "w") as f:
            if each_transaction:
                f.write("Transaction Log Summary\n")
                f.write("======================\n\n")

                # write now time
                now = datetime.now()
                f.write(f"Current Time: {now.strftime('%Y-%m-%d %H:%M:%S:%f')}\n\n")

            for transaction_id, transaction in self.transactions.items():
                transaction_latency = "Not finished"
                # f.write(
                #         f"123Transaction {transaction_id} end time: {transaction.end_time}, start time: {transaction.start_time}\n"
                #     )
                
                # f.write(
                #         f"33Transaction {transaction_id} end time: {self.transactions[transaction_id].end_time}, start time: {self.transactions[transaction_id].start_time}\n"
                #     )
                if transaction.status == "Completed":
 
                    transaction_latency = (
                        transaction.end_time - transaction.start_time
                    ).total_seconds() * 1000000  # in microseconds
                    total_latency += transaction_latency
                    total_transactions += 1
                    total_latency_by_type[
                        transaction.transaction_type
                    ] += transaction_latency
                    count_by_type[transaction.transaction_type] += 1

                if each_transaction:
                    f.write(f"Transaction ID: {transaction_id}\n")
                    f.write(
                        f"    Transaction Type: {transaction.transaction_type.name}, Latency (us): {transaction_latency}\n"
                    )

                for hop_id, hop in transaction.hops.items():
                    if not hop.status == "Completed":
                        f.write(
                            f"Not Completed Hops:    Hop :{hop_id}, Hop executor server: {hop.hop_executor_server}, Status: {hop.status}, Transaction Type: {transaction.transaction_type.name}\n"
                        )
                    hop_latency = "Not finished"
                    if hop.end_time:
                        hop_latency = (
                            hop.end_time - hop.start_time
                        ).total_seconds() * 1000000
                        if hop.hop_id == 1:
                            total_first_hop_latency_by_type[
                                transaction.transaction_type
                            ] += hop_latency
                            first_hop_count_by_type[transaction.transaction_type] += 1
                        elif hop.hop_id == 2:
                            total_second_hop_latency_by_type[
                                transaction.transaction_type
                            ] += hop_latency
                            second_hop_count_by_type[transaction.transaction_type] += 1

                    queue_wait_time_by_hop[transaction.transaction_type][
                        hop_id
                    ] += hop.get_total_queue_wait_time()
                    websocket_transmission_time_by_hop[transaction.transaction_type][
                        hop_id
                    ] += hop.get_total_websocket_transmission_time()
                    queue_wait_count_by_hop[transaction.transaction_type][
                        hop_id
                    ] += hop.get_queue_wait_count()
                    queue_wait_count_by_hop_2[transaction.transaction_type][hop_id] += 1
                    websocket_transmission_count_by_hop[transaction.transaction_type][
                        hop_id
                    ] += hop.get_websocket_transmission_count()
                    websocket_transmission_count_by_hop_2[transaction.transaction_type][
                        hop_id
                    ] += 1

            f.write("\nOverall Summary\n")
            f.write("---------------\n")
            avg_transaction_latency = (
                total_latency / total_transactions if total_transactions > 0 else 0
            )
            f.write(
                f"Average Transaction Latency: {avg_transaction_latency:.2f} microseconds\n\n"
            )

            f.write("Average Latency by Hop for Each Transaction Type\n")
            f.write(
                "--------------------------------------------------------------------------------------------------------------------\n"
            )
            f.write(
                "| Definition of Latency: End Time - Start Time (microseconds)                                                      | \n"
            )
            f.write(
                "| Formula:  Transaction Latency = Sum of Hop Latencies                                                             | \n"
            )
            for transaction_type in TransactionType:
                avg_latency = (
                    (
                        total_latency_by_type[transaction_type]
                        / count_by_type[transaction_type]
                    )
                    if count_by_type[transaction_type] > 0
                    else 0
                )
                avg_first_hop_latency = (
                    (
                        total_first_hop_latency_by_type[transaction_type]
                        / first_hop_count_by_type[transaction_type]
                    )
                    if first_hop_count_by_type[transaction_type] > 0
                    else 0
                )
                avg_second_hop_latency = (
                    (
                        total_second_hop_latency_by_type[transaction_type]
                        / second_hop_count_by_type[transaction_type]
                    )
                    if second_hop_count_by_type[transaction_type] > 0
                    else 0
                )
                f.write(
                    f"| {transaction_type.name} - Average Transaction Latency: {avg_latency:10.2f} , First Hop Latency: {avg_first_hop_latency:10.2f} , Second Hop Latency: {avg_second_hop_latency:10.2f}    |\n"
                )
            f.write(
                "--------------------------------------------------------------------------------------------------------------------\n\n"
            )

            f.write("\nStatistics by Transaction Type\n")
            f.write(
                "--------------------------------------------------------------------------------------------------------------------\n"
            )
            f.write(
                "| Definition of Waiting Time: Queue Exit Time - Enter Time or WebSocket Received Time - Send Time (microseconds)   | \n"
            )
            f.write(
                "| Formula:  Hop Latency = (Queue: Wait Avg Time * Wait Avg Ct) + (WebSockets: Avg Time * Avg Ct) + SQL Avg Time    | \n"
            )
            for transaction_type in TransactionType:
                for hop_id in [1, 2]:
                    avg_queue_wait_time = queue_wait_time_by_hop[transaction_type][
                        hop_id
                    ] / (
                        queue_wait_count_by_hop[transaction_type][hop_id]
                        if queue_wait_count_by_hop[transaction_type][hop_id] > 0
                        else 1
                    )
                    avg_websocket_transmission_time = (
                        websocket_transmission_time_by_hop[transaction_type][hop_id]
                        / websocket_transmission_count_by_hop[transaction_type][hop_id]
                        if websocket_transmission_count_by_hop[transaction_type][hop_id]
                        > 0
                        else 1
                    )
                    if avg_queue_wait_time:
                        f.write(
                            f"| {transaction_type.name} Hop {hop_id} -      Queue Wait: Avg Time: {avg_queue_wait_time:10.2f} , Avg Count: {queue_wait_count_by_hop[transaction_type][hop_id]/queue_wait_count_by_hop_2[transaction_type][hop_id] if queue_wait_count_by_hop_2[transaction_type][hop_id] > 0 else 1:3.2f}                                               | \n"
                        )
                    if (
                        avg_websocket_transmission_time
                        and websocket_transmission_count_by_hop[transaction_type][
                            hop_id
                        ]
                    ):
                        f.write(
                            f"|                 WebSockets: Avg Time: {avg_websocket_transmission_time:10.2f} , Avg Count: {websocket_transmission_count_by_hop[transaction_type][hop_id]/websocket_transmission_count_by_hop_2[transaction_type][hop_id] if websocket_transmission_count_by_hop_2[transaction_type][hop_id] > 0 else 1:3.2f}                                               | \n"
                        )
            f.write(
                "--------------------------------------------------------------------------------------------------------------------\n"
            )
            f.write("\n")
