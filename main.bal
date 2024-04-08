import ballerina/io;
import ballerina/random;
import ballerinax/mongodb;

configurable string host = ?;
configurable string database = ?;
configurable string resultHost = ?;
const string lotteryCollection = "lottery";

final mongodb:Client mongoDb = check new ({
    connection: host
});

public function main() returns error? {
    final mongodb:Database Db = check mongoDb->getDatabase(database);

    int num1 = check random:createIntInRange(1, 100);
    int num2 = check random:createIntInRange(1, 100);
    int num3 = check random:createIntInRange(1, 100);
    int num4 = check random:createIntInRange(1, 100);

    // Generating the winning lottery number 
    string lotteryNumber = num1.toString() + ":" + num2.toString() + ":" + num3.toString() + ":" + num4.toString();
    io:println(lotteryNumber);

    mongodb:Collection lotteryCol = check Db->getCollection(lotteryCollection);
    stream<Lottery, error?> findResult = check lotteryCol->find();
    Lottery[] result = check from Lottery m in findResult
        select m;

    foreach Lottery item in result {
        item.enabled = true;
        item.last_draw_bet_value = item.value;
        item.last_draw_value = lotteryNumber;
        item.value = "";
        if item.value == lotteryNumber {
            item.winner = true;
        } else {
            item.winner = false;
        }
        boolean res = check updateLotteryBet(Db, item);
        if (!res) {
            io:println("Error updating bet record of " + item.email);
        }
    }
    io:println("Scheduler job done!");
}

isolated function updateLotteryBet(mongodb:Database Db, Lottery lot) returns boolean|error {
    mongodb:Collection lotteryCol = check Db->getCollection(lotteryCollection);
    mongodb:UpdateResult updateResult = check lotteryCol->updateOne({email: lot.email}, {
        set: lot
    });
    if updateResult.modifiedCount != 1 {
        return error(string `Failed to update the credits with email ${lot.email}`);
    }
    return true;
}

public type LoteryInput record {|
    string value;
    string email;
    string last_draw_bet_value?;
    string last_draw_value?;
    boolean enabled?;
    boolean winner?;
|};

public type LotteryUpdate record {|
    string value;
    string email;
|};

public type Lottery record {|
    readonly string id;
    *LoteryInput;
|};
