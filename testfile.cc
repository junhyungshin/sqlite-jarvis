#include <string>
#include <cstring>
#include <iostream>
#include <sstream>
#include <unistd.h>

#include "DBFile.h"
#include "Record.h"
#include "Catalog.h"
#include "Schema.h"

using namespace std;
// 2 arguements - first - page number, second - record number
int main (int argc, char* argv[]) {
  off_t whichRecord = 0, whichPage = 0;
  if (argc == 3) {
    whichPage = atoi(argv[1]);
    whichRecord = atoi(argv[2]);
  }
  string catalogFileName = "catalog.sqlite";
	Catalog catalog(catalogFileName);

  Schema schema;
  string tableName = "lineitem";
  catalog.GetSchema(tableName, schema);

  DBFile dbFile;
  string dbFilePath;
  catalog.GetDataFile(tableName, dbFilePath);

  char* dbFilePathC = new char[dbFilePath.length()+1];
  strcpy(dbFilePathC, dbFilePath.c_str());

  if(dbFile.Open(dbFilePathC) == -1) {
    cerr << endl << "ERROR: File Can't be opened" << endl;
    exit(-1);
  }

  Record rec;
  if (dbFile.GetRecord(rec, whichPage, whichRecord) == 0) {
    stringstream ss;
    rec.print(ss, schema);
    cout << endl << ss.str() << endl;
  }
  else {
    cout << endl << "ERROR: Some error occured" << endl;
  }
  dbFile.Close();
  return 0;
}
