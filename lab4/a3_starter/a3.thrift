service KeyValueService {
  string get(1: string key);
  void put(1: string key, 2: string value);
  void backupPut(1: string key, 2: string value);
  void setPrimary(1: bool isPrimary);
  void sync(1: map<string, string> data);

}
