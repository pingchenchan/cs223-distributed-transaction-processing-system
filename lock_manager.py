import asyncio
from history_table import TransactionType

class LockManager:
    def __init__(self):
        self.lockA = asyncio.Lock()
        self.lockB = asyncio.Lock()

    async def acquire_all_locks(self, transaction_type):
        if transaction_type in [TransactionType.T3, TransactionType.T4]:
            if self.lockB.locked():
                return False  # Fail if lockB is already acquired
            else:
                await self.lockA.acquire()  # Acquire lockA
                return True  # Success
        elif transaction_type == TransactionType.T7:
            if self.lockA.locked():
                return False  # Fail if lockA is already acquired
            else:
                # No need to acquire lockA for T7 as it's only checking lockA's status.
                return True  # Success
        else:
            return True  # No need to acquire locks for other transaction types

    def release_locks(self, transaction_type):
        if transaction_type in [TransactionType.T3, TransactionType.T4] and self.lockA.locked():
            self.lockA.release()
        elif transaction_type == TransactionType.T7 and self.lockB.locked():
            self.lockB.release()

# Usage example
async def perform_transaction(transaction_type, lock_manager):
    if await lock_manager.acquire_all_locks(transaction_type):
        try:
            # Transaction logic goes here
            print(f"Transaction {transaction_type} is executing")
        finally:
            lock_manager.release_locks(transaction_type)
    else:
        print(f"Transaction {transaction_type} failed to acquire necessary locks")








