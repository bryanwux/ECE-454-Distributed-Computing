exception IllegalArgument {
  1: string message;
}

service BcryptService {
 list<string> hashPassword (1: list<string> password, 2: i16 logRounds) throws (1: IllegalArgument e);
 list<bool> checkPassword (1: list<string> password, 2: list<string> hash) throws (1: IllegalArgument e);
 list<string> hashPasswordComp (1: list<string> password, 2: i16 logRounds) throws (1: IllegalArgument e);
 list<bool> checkPasswordComp (1: list<string> password, 2: list<string> hash) throws (1: IllegalArgument e);
 void BENodeHandler(1: string BEHost, 2: i32 BEPort) throws (1:IllegalArgument e);
}
