#include <string>
#include <cstring>
#include <iostream>
#include <sstream>
#include <unistd.h>
#include <vector>
#include <algorithm>
#include <iterator>

#include "DBFile.h"
#include "Record.h"
#include "Catalog.h"
#include "Schema.h"
#include "BPlusTree.h"

using namespace std;

int main (int argc, char* argv[]) {

  // vector<int> a{1,2,3,4,5,6};
  // vector<int> b;
  // b.reserve(4);
  // copy(a.begin() + 2, a.end(), back_inserter(b));
  // a.erase(a.begin()+2, a.end());

  string tableName = "orders";
  string att = "o_custkey";

  string catalogFileName = "catalog.sqlite";
	Catalog catalog(catalogFileName);

  Schema schema;
  catalog.GetSchema(tableName, schema);

  string dbFilePath;
  catalog.GetDataFile(tableName, dbFilePath);

  char* dbFilePathC = new char[dbFilePath.length()+1];
  strcpy(dbFilePathC, dbFilePath.c_str());

  DBFile dbFile;
  if(dbFile.Open(dbFilePathC) == -1) {
    cerr << endl << "ERROR: File Can't be opened" << endl;
    exit(-1);
  }


  int whichAtt = schema.Index(att);

  BPlusTree bpt;
  Record rec;
  int recidx = 0, pageidx = dbFile.GetCurrentPageNum();

  while(dbFile.GetNext(rec) == 0) {

    //Increment pageidx and recidx
    if (pageidx < dbFile.GetCurrentPageNum()) {
      pageidx = dbFile.GetCurrentPageNum();
      recidx = 0;
    }
    else recidx++;

    //Extract key
    char* _bits = rec.GetBits();
    int key = *((int*) (_bits + ((int*) _bits)[whichAtt + 1]));

    //Insert in B+ tree
    bpt.InsertKey(key, pageidx, recidx);

  }

  dbFile.Close();

  //dont know how to check GetNext
  Node* node;
  while(bpt.GetNext(node)) {
    int size = node->keys.size();
    string isLeaf = node->isLeaf ? "A Leaf Node" : "An Internal Node";
    cout <<endl << isLeaf << " with first element = " << node->keys.front()
    << " and last element = "<< node->keys.back()<< " and size = "
    << node->keys.size() <<endl;
  }
  cout << endl;
  return 0;
}
