import ballerina/io;
import ballerina/random;
import ballerina/sql;
import ballerinax/mongodb;
import ballerinax/mysql;
import ballerinax/mysql.driver as _;

configurable string host = ?;
configurable string database = ?;
const string lotteryCollection = "lottery";

configurable string mysqlHost = ?;
configurable string mysqlUser = ?;
configurable string mysqlPassword = ?;
configurable int mysqlPort = ?;

configurable string dbType = ?;

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

    Lottery[] lottery = [];
    if dbType == "mysql" {
        mysql:Client mysqlDb = check getMysqlConnection();
        stream<Lottery, error?> resultStream = mysqlDb->query(`SELECT id, bet_value as value,last_draw_bet_value,last_draw_value,enabled,winner,email FROM Lottery`);
        check from Lottery rw in resultStream
            do {
                lottery.push(rw);
            };
        check resultStream.close();

    } else {
        mongodb:Collection lotteryCol = check Db->getCollection(lotteryCollection);
        stream<Lottery, error?> findResult = check lotteryCol->find();
        lottery = check from Lottery m in findResult
            select m;
    }

    foreach Lottery item in lottery {
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
    if dbType == "mysql" {
        mysql:Client mysqlDb = check getMysqlConnection();
        sql:ParameterizedQuery query = `UPDATE Lottery
                                        SET bet_value = ${lot.value}, last_draw_bet_value = ${lot.last_draw_bet_value}, last_draw_value = ${lot.last_draw_value}, enabled = ${lot.enabled}, winner = ${lot.winner}
                                        WHERE email = ${lot.email};`;
        sql:ExecutionResult result = check mysqlDb->execute(query);
        return true;
    } else {
        mongodb:Collection lotteryCol = check Db->getCollection(lotteryCollection);
        mongodb:UpdateResult updateResult = check lotteryCol->updateOne({email: lot.email}, {
            set: lot
        });
        if updateResult.modifiedCount != 1 {
            return error(string `Failed to update the credits with email ${lot.email}`);
        }
        return true;
    }
}

isolated function getMysqlConnection() returns mysql:Client|sql:Error {
    final mysql:Client|sql:Error dbClient = new (
        host = mysqlHost, user = mysqlUser, password = mysqlPassword, port = mysqlPort, database = database
    );
    return dbClient;
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
