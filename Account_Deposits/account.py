class Account:
    def __init__(self) -> None:
        self.balance = 0

    def depositFund(self, deposit_amount):
        self.balance += deposit_amount
        return f"Deposit of ${deposit_amount} made. New balance is: ${self.balance}"


    def withdrawFund(self, withdrawal_amount):
        if (self.balance - withdrawal_amount) < 0:
            return f"Insufficient funds. Current balance is: ${self.balance}"
        else:
            self.balance -= withdrawal_amount
            return f"Withdrawal of ${withdrawal_amount} processed. New balance is: ${self.balance}"
    
dimasAccount = Account()
print(dimasAccount.depositFund(1500))
print(dimasAccount.withdrawFund(150))
print(dimasAccount.withdrawFund(1350.01))